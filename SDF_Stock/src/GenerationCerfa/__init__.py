import logging
import azure.functions as func
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
import requests
import urllib.parse
import json
import base64
import io
import re
from datetime import datetime, timezone

from PIL import Image
from pypdf import PdfReader, PdfWriter, Transformation
from pypdf.generic import NameObject, TextStringObject
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4, landscape
from reportlab.lib.styles import ParagraphStyle
from reportlab.platypus import Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle

# --------- CONFIG GLOBALE -------------
VAULT_URL = "https://events-manager-kv.vault.azure.net/"
TEMPLATE_FOLDER = "En 1 Clic/Cerfa"
TARGET_FOLDER = "04 - Documents affaires"
TYPE_DOC = "CERFA"

# Un modèle par entité : le bloc prestataire (raison sociale, dépôts, assurance,
# conseiller sécurité) est pré-rempli dans le PDF, la fonction ne remplit que le spectacle.
TEMPLATES = {
    "BASSIN PARISIEN": "Modèle Cerfa - Bassin Parisien.pdf",
    "CENTRE FRANCE": "Modèle Cerfa - Centre France.pdf",
    "SUD OUEST": "Modèle Cerfa - Sud Ouest.pdf",
}

# Noms exacts des champs AcroForm du CERFA 14098 (identiques dans les 3 modèles).
CHAMP_PREFECTURE = "Préfecture"
CHAMP_COMMUNE = "Commune"
CHAMP_DATE_TIR = "Date du tir_es_:date"
CHAMP_MASSE_ACTIVE = "kg"
CHAMP_CALIBRE_MAX = "Calibre maximum mis en œuvre durant le spectacle pyrotechnique"
CHAMP_DECLARANT = "Je déclare sur lhonneur M ou Mme Représentant du prestataire"
CHAMP_DATE_DECLARATION = "date_es_:date"
# Section 7 « SIGNATURE », page 4 : « Signature et cachet du représentant légal du prestataire ».
CHAMP_SIGNATURE = "Signature26_es_:signer:signature"

CASE_MASSE_SUP_35 = "Check Box10"
CASE_MASSE_INF_35 = "Check Box11"
CASE_NIVEAU_1 = "Check Box15"
CASE_NIVEAU_2 = "Check Box16"
CASE_SANS_NIVEAU = "Check Box17"

ETAT_COCHE = "Oui"
ETAT_DECOCHE = "Off"

SEUIL_MASSE_ACTIVE = 35      # kg : au-delà, la case "plus de 35 kg" est cochée
SEUIL_CALIBRE_NIVEAU_2 = 100  # mm : à partir de ce calibre, niveau 2 au lieu de niveau 1

# --- Annexe « liste des produits » du dossier préfecture -----------------------------
# Catalogue complet filtré sur la distance de sécurité du spectacle, comme dans l'outil
# Excel : ce ne sont pas les produits du devis, mais ceux susceptibles d'être mis en œuvre.
TYPE_DOC_PRODUITS = "Liste produits CERFA"
COLONNES_PRODUITS = [
    ("Désignation produit", "Description_ukoba", 330),
    ("Calibre", "Cal", 60),
    ("Classification", "Cl", 90),
    ("Numéro de certification", "Num_agrement", 160),
    ("Distance de sécurité", "Dist_securite", 110),
]
DISTANCE_PAR_DEFAUT = 1000  # mètres, repli de l'outil Excel quand la distance est vide
BLEU_SDF = colors.HexColor("#0094D2")
# --------------------------------------

session = requests.Session()
adapter = requests.adapters.HTTPAdapter(pool_connections=100, pool_maxsize=100)
session.mount('https://', adapter)


def get_secret(name: str):
    credential = DefaultAzureCredential()
    client = SecretClient(vault_url=VAULT_URL, credential=credential)
    return client.get_secret(name).value


def get_graph_token(tenant_id, client_id, client_secret):
    url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
    data = {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret,
        "scope": "https://graph.microsoft.com/.default"
    }
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    try:
        response = session.post(url, data=data, headers=headers)
        response.raise_for_status()
        return response.json().get("access_token")
    except Exception as e:
        logging.error(f"Erreur token: {e}")
        return None


def graph_filtered_items(site_id, list_id, token, filter_expr=None):
    base_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/items?$expand=fields"
    headers = {
        "Authorization": f"Bearer {token}",
        "Prefer": "HonorNonIndexedQueriesWarningMayFailRandomly"
    }
    if filter_expr:
        filter_param = urllib.parse.quote(filter_expr, safe="=()/ '")
        base_url += f"&$filter={filter_param}"

    results = []
    url = base_url
    while url:
        res = session.get(url, headers=headers)
        if not res.ok:
            logging.error(f"Erreur API Graph (filtré). Status: {res.status_code}. Réponse: {res.text}")
        res.raise_for_status()
        data = res.json()
        results.extend(data.get("value", []))
        url = data.get("@odata.nextLink")
    return [item.get("fields", {}) for item in results]


def graph_get_item_by_id(site_id, list_id, item_id, token):
    url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/items/{item_id}?$expand=fields"
    headers = {"Authorization": f"Bearer {token}"}
    try:
        res = session.get(url, headers=headers)
        res.raise_for_status()
        return res.json().get("fields", {})
    except Exception as e:
        logging.error(f"Erreur Get Item: {e}")
        return None


def graph_post_item(site_id, list_id, token, fields_dict):
    url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/items"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    res = session.post(url, headers=headers, json={"fields": fields_dict})
    if not res.ok:
        logging.error(f"Erreur create item: {res.status_code} {res.text}")
    res.raise_for_status()
    return res.json().get("id")


def download_graph_file(site_id, token, filepath):
    url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drive/root:/{urllib.parse.quote(filepath)}:/content"
    res = session.get(url, headers={"Authorization": f"Bearer {token}"})
    res.raise_for_status()
    return res.content


def ensure_folder(site_id, token, folder_name):
    """Crée le dossier de dépôt s'il n'existe pas encore (idempotent)."""
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    url_check = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drive/root:/{urllib.parse.quote(folder_name)}"
    if session.get(url_check, headers={"Authorization": f"Bearer {token}"}).ok:
        return
    payload = {"name": folder_name, "folder": {}, "@microsoft.graph.conflictBehavior": "fail"}
    res = session.post(
        f"https://graph.microsoft.com/v1.0/sites/{site_id}/drive/root/children",
        headers=headers, json=payload
    )
    if not res.ok and res.status_code != 409:
        logging.warning(f"Création du dossier {folder_name} impossible : {res.status_code} {res.text[:200]}")


# ======================================================================================
# Helpers métier
# ======================================================================================
def initiales(chaine):
    """Reproduit la fonction Initiale() de l'outil Excel : 'Guillaume LECOQ' -> 'GL'."""
    if not chaine:
        return ""
    mots = re.split(r"[\s\-']+", str(chaine).strip())
    return "".join(m[0].upper() for m in mots if m)


def texte(valeur):
    return "" if valeur is None else str(valeur).strip()


def nombre(valeur):
    """Convertit une valeur SharePoint en float, en tolérant les nombres au format texte."""
    if valeur in (None, ""):
        return None
    try:
        return float(str(valeur).replace(",", "."))
    except (TypeError, ValueError):
        return None


def entier_texte(valeur):
    """75.0 -> '75' ; 100 -> '100' ; None -> ''."""
    val = nombre(valeur)
    if val is None:
        return ""
    return str(int(val)) if float(val).is_integer() else str(val)


def format_date(valeur):
    """'2026-07-13T00:00:00Z' -> '13/07/2026'."""
    brut = texte(valeur)
    if not brut:
        return ""
    try:
        return datetime.strptime(brut[:10], "%Y-%m-%d").strftime("%d/%m/%Y")
    except ValueError:
        return brut


def format_masse_active(valeur):
    """103.31 -> '103,31 Kg' (format repris de l'outil Excel)."""
    val = nombre(valeur)
    if val is None:
        return ""
    return f"{val:.2f}".replace(".", ",") + " Kg"


def annee_devis(devis):
    """Année portée par le numéro de devis : 'DV-2024-GL-CHATEAUFORT78' -> 2024."""
    match = re.match(r"DV[-_](\d{4})", texte(devis.get("Title")).upper())
    return int(match.group(1)) if match else 0


def choisir_devis(devis_list):
    """
    Règle métier : on retient le dernier devis en date de l'affaire.

    'Created' n'est pas fiable (l'import initial a créé le même jour des devis
    de millésimes différents), on trie donc sur l'année du numéro de devis puis
    sur l'ID SharePoint. Le devis fictif DV_CREATION_AFFAIRE est écarté.
    """
    reels = [d for d in devis_list if not texte(d.get("Title")).upper().startswith("DV_CREATION")]
    if not reels:
        return None
    return max(reels, key=lambda d: (annee_devis(d), int(nombre(d.get("id")) or 0)))


def selectionner_produits(produits, distance_max):
    """
    Règle reprise de l'outil Excel : on retient les produits dont la distance de sécurité
    est inférieure ou égale à celle du spectacle, en écartant toute ligne dont l'une des
    cinq colonnes de l'annexe est vide.
    """
    retenus = []
    for produit in produits:
        if any(produit.get(champ) in (None, "") for _, champ, _ in COLONNES_PRODUITS):
            continue
        distance = nombre(produit.get("Dist_securite"))
        if distance is None or distance > distance_max:
            continue
        retenus.append(produit)
    return retenus


def construire_pdf_produits(produits, masse_active, categories):
    """Annexe « liste des produits » en PDF paysage, en-tête de tableau répété à chaque page."""
    tampon = io.BytesIO()
    document = SimpleDocTemplate(
        tampon, pagesize=landscape(A4),
        leftMargin=28, rightMargin=28, topMargin=28, bottomMargin=28,
        title="Liste des produits", author="Soirs de Fêtes")

    style_titre = ParagraphStyle("titre", fontName="Helvetica", fontSize=10, leading=14)
    style_cellule = ParagraphStyle("cellule", fontName="Helvetica", fontSize=7.5, leading=9)

    lignes = [[libelle for libelle, _, _ in COLONNES_PRODUITS]]
    for produit in produits:
        lignes.append([
            Paragraph(texte(produit.get("Description_ukoba")), style_cellule),
            entier_texte(produit.get("Cal")),
            texte(produit.get("Cl")),
            texte(produit.get("Num_agrement")),
            entier_texte(produit.get("Dist_securite")),
        ])

    tableau = Table(lignes, colWidths=[largeur for _, _, largeur in COLONNES_PRODUITS],
                    repeatRows=1)
    tableau.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), BLEU_SDF),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTSIZE", (0, 0), (-1, -1), 7.5),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("ALIGN", (1, 1), (-1, -1), "CENTER"),
        ("GRID", (0, 0), (-1, -1), 0.25, colors.HexColor("#9D9C9C")),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, colors.HexColor("#F2F9FD")]),
        ("TOPPADDING", (0, 0), (-1, -1), 2),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 2),
    ]))

    document.build([
        Paragraph(f"<b>Types d'artifice :</b> {categories}", style_titre),
        Paragraph(f"<b>Quantité totale de matière active :</b> {masse_active}", style_titre),
        Spacer(1, 10),
        tableau,
    ])
    return tampon.getvalue()


def deposer_fichier(site_id, token, nom_fichier, contenu, content_type):
    """Dépose le fichier dans le dossier cible et renvoie (drive_item_id, file_list_item_id)."""
    url_upload = (f"https://graph.microsoft.com/v1.0/sites/{site_id}/drive/root:/"
                  f"{TARGET_FOLDER}/{urllib.parse.quote(nom_fichier, safe='')}:/content")
    res = session.put(url_upload, headers={"Authorization": f"Bearer {token}",
                                           "Content-Type": content_type}, data=contenu)
    res.raise_for_status()
    drive_item_id = res.json().get("id", "")

    file_list_item_id = ""
    try:
        url_list_item = (f"https://graph.microsoft.com/v1.0/sites/{site_id}"
                         f"/drive/items/{drive_item_id}?$expand=listItem")
        res_li = session.get(url_list_item, headers={"Authorization": f"Bearer {token}"})
        if res_li.ok:
            file_list_item_id = res_li.json().get("listItem", {}).get("id", "")
    except Exception as e:
        logging.warning(f"Impossible de récupérer le listItemId de {nom_fichier} : {e}")
    return drive_item_id, file_list_item_id


def reponse_erreur(code, message, status_code, **extra):
    """Réponse d'erreur normalisée : `message` est directement affichable dans l'app."""
    logging.warning(f"{code} : {message}")
    corps = {"status": "error", "code": code, "message": message}
    corps.update(extra)
    return func.HttpResponse(json.dumps(corps, ensure_ascii=False),
                             status_code=status_code, mimetype="application/json")


# ======================================================================================
# Remplissage du formulaire PDF
# ======================================================================================
def nom_complet_champ(annot):
    """Reconstitue le nom complet d'un champ en remontant la chaîne des parents."""
    parties = []
    courant = annot
    vus = set()
    while courant is not None and id(courant) not in vus:
        vus.add(id(courant))
        partiel = courant.get("/T")
        if partiel:
            parties.insert(0, str(partiel))
        parent = courant.get("/Parent")
        courant = parent.get_object() if parent else None
    return ".".join(parties)


def type_champ(annot):
    courant = annot
    vus = set()
    while courant is not None and id(courant) not in vus:
        vus.add(id(courant))
        if "/FT" in courant:
            return str(courant["/FT"])
        parent = courant.get("/Parent")
        courant = parent.get_object() if parent else None
    return ""


def remplir_pdf(template_bytes, valeurs):
    """
    Renseigne les champs AcroForm du modèle et renvoie le PDF résultant.

    Les champs texte passent par pypdf, qui régénère leur flux d'apparence : le texte
    reste visible dans les lecteurs qui ignorent NeedAppearances (Chrome notamment).
    Les cases à cocher sont positionnées à la main (/V et /AS), leur état coché
    valant 'Oui' dans le CERFA 14098.
    """
    reader = PdfReader(io.BytesIO(template_bytes))
    writer = PdfWriter(clone_from=reader)
    writer.set_need_appearances_writer(True)

    renseignes = set()
    for page in writer.pages:
        textes = {}
        for annot_ref in page.get("/Annots", []) or []:
            annot = annot_ref.get_object()
            nom = nom_complet_champ(annot)
            if nom not in valeurs:
                continue
            valeur = valeurs[nom]
            if type_champ(annot) == "/Btn":
                etat = NameObject(f"/{valeur}" if valeur != ETAT_DECOCHE else "/Off")
                annot[NameObject("/V")] = etat
                annot[NameObject("/AS")] = etat
            else:
                textes[nom] = valeur
            renseignes.add(nom)
        if textes:
            writer.update_page_form_field_values(page, textes, auto_regenerate=False)

    manquants = sorted(set(valeurs) - renseignes)
    if manquants:
        logging.warning(f"Champs absents du modèle CERFA, ignorés : {manquants}")

    sortie = io.BytesIO()
    writer.write(sortie)
    return sortie.getvalue()


def rectangle_champ(reader, nom_champ):
    """Renvoie (index_page, x0, y0, x1, y1) du champ demandé, ou None."""
    for index, page in enumerate(reader.pages):
        for annot_ref in page.get("/Annots", []) or []:
            annot = annot_ref.get_object()
            if nom_complet_champ(annot) == nom_champ and "/Rect" in annot:
                r = [float(v) for v in annot["/Rect"]]
                return index, min(r[0], r[2]), min(r[1], r[3]), max(r[0], r[2]), max(r[1], r[3])
    return None


def apposer_signature(pdf_bytes, image_bytes):
    """
    Incruste l'image de signature dans le cadre du champ de signature du CERFA.

    L'image est réduite pour tenir dans le cadre en conservant ses proportions,
    puis centrée. Renvoie (pdf, True) si la signature a été posée.
    """
    reader = PdfReader(io.BytesIO(pdf_bytes))
    cadre = rectangle_champ(reader, CHAMP_SIGNATURE)
    if cadre is None:
        logging.warning(f"Champ « {CHAMP_SIGNATURE} » absent du modèle : signature non apposée.")
        return pdf_bytes, False

    index, x0, y0, x1, y1 = cadre
    largeur_cadre, hauteur_cadre = x1 - x0, y1 - y0

    image = Image.open(io.BytesIO(image_bytes))
    if image.mode not in ("RGB", "L"):
        image = image.convert("RGB")
    echelle = min(largeur_cadre / image.width, hauteur_cadre / image.height)
    largeur, hauteur = image.width * echelle, image.height * echelle

    # Pillow fixe la taille de page du PDF via la résolution : largeur_pt = largeur_px / dpi * 72
    calque = io.BytesIO()
    image.save(calque, "PDF", resolution=image.width * 72.0 / largeur)
    calque.seek(0)

    # Calée à gauche du cadre, sous le libellé « Signature et cachet », et centrée en hauteur.
    writer = PdfWriter(clone_from=reader)
    writer.pages[index].merge_transformed_page(
        PdfReader(calque).pages[0],
        Transformation().translate(x0 + 4, y0 + (hauteur_cadre - hauteur) / 2),
    )
    sortie = io.BytesIO()
    writer.write(sortie)
    logging.info(f"Signature apposée page {index + 1}, {largeur:.0f}x{hauteur:.0f} pt.")
    return sortie.getvalue(), True


# ======================================================================================
# Point d'entrée
# ======================================================================================
def main(req: func.HttpRequest) -> func.HttpResponse:
    try:
        try:
            body = req.get_json()
        except ValueError:
            return reponse_erreur("payload_invalide", "Le corps de la requête doit être un JSON valide.", 400)

        id_affaire = body.get("ID_evt") or body.get("ID_aff")
        id_devis_force = body.get("ID_devis")
        interlocuteur_fourni = texte(body.get("interlocuteur"))
        signature_base64 = texte(body.get("signature_base64"))

        logging.info(f"Paramètres reçus : ID_evt={id_affaire}, ID_devis={id_devis_force}, "
                     f"interlocuteur={interlocuteur_fourni or '(déduit du client)'}, "
                     f"signature={'fournie' if signature_base64 else 'absente'}")
        if not id_affaire:
            return reponse_erreur("parametre_manquant", "Le paramètre 'ID_evt' est requis.", 400)
        if nombre(id_affaire) is None:
            return reponse_erreur("parametre_invalide", "Le paramètre 'ID_evt' doit être numérique.", 400)
        id_affaire = entier_texte(id_affaire)

        tenant_id = get_secret("tenantid")
        client_id = get_secret("clientid")
        client_secret = get_secret("appsecret")
        site_id = get_secret("siteid")

        affaire_list_id = get_secret("affaireevtslistid")
        devis_list_id = get_secret("devislistid")
        clients_list_id = get_secret("clientslistid")
        prefectures_list_id = get_secret("prefectureslistid")
        produits_list_id = get_secret("produitslistid")
        affaire_doc_list_id = get_secret("affaireevtsdoclistid")

        token = get_graph_token(tenant_id, client_id, client_secret)
        if not token:
            return reponse_erreur("authentification", "Échec de l'authentification Graph.", 500)

        # 1. Affaire / événement
        affaire = graph_get_item_by_id(site_id, affaire_list_id, id_affaire, token)
        if not affaire:
            return reponse_erreur("affaire_introuvable",
                                  f"L'affaire {id_affaire} est introuvable.", 404)

        entite = texte(affaire.get("Entite"))
        nom_evt = texte(affaire.get("Title"))

        # 2. Modèle CERFA de l'entité
        modele = TEMPLATES.get(entite.upper())
        if not modele:
            return reponse_erreur(
                "entite_sans_modele",
                f"Aucun modèle CERFA n'est disponible pour l'entité « {entite or 'non renseignée'} ». "
                f"Entités couvertes : {', '.join(sorted(TEMPLATES))}.",
                422, entite=entite, ID_evt=id_affaire)

        # 3. Devis : celui imposé par l'appel, sinon le dernier en date de l'affaire
        if id_devis_force:
            devis = graph_get_item_by_id(site_id, devis_list_id, entier_texte(id_devis_force), token)
            if not devis:
                return reponse_erreur("devis_introuvable",
                                      f"Le devis {id_devis_force} est introuvable.", 404)
            if entier_texte(devis.get("Aff_ID")) not in ("", id_affaire):
                logging.warning(f"Le devis {id_devis_force} est rattaché à l'affaire "
                                f"{devis.get('Aff_ID')} et non à {id_affaire}.")
        else:
            try:
                candidats = graph_filtered_items(site_id, devis_list_id, token,
                                                 f"fields/Aff_ID eq {id_affaire}")
            except Exception:
                candidats = graph_filtered_items(site_id, devis_list_id, token,
                                                 f"fields/Aff_ID eq '{id_affaire}'")
            devis = choisir_devis(candidats)
            if not devis:
                return reponse_erreur(
                    "devis_absent",
                    f"Aucun devis n'est rattaché à l'affaire « {nom_evt or id_affaire} ». "
                    "Le CERFA ne peut pas être généré sans devis : il en tire la masse active, "
                    "le calibre maximum et le département.",
                    422, ID_evt=id_affaire)
            if len(candidats) > 1:
                logging.info(f"{len(candidats)} devis pour l'affaire {id_affaire}, "
                             f"retenu : {devis.get('Title')} (id {devis.get('id')})")

        # 4. Préfecture : le n° de département est porté par Devis.Reference
        num_departement = nombre(devis.get("Reference"))
        prefecture = ""
        if num_departement:
            departements = graph_filtered_items(
                site_id, prefectures_list_id, token,
                f"fields/Num_departement eq {int(num_departement)}")
            nom_departement = texte(departements[0].get("Title")) if departements else ""
            if not nom_departement:
                logging.warning(f"Département {int(num_departement)} absent de la liste Prefectures.")
            prefecture = f"{int(num_departement)} - {nom_departement}" if nom_departement \
                else str(int(num_departement))
        else:
            logging.warning(f"Devis {devis.get('Title')} sans département (colonne Reference).")

        # 5. Interlocuteur SdF : celui transmis par l'appelant fait foi — c'est lui qui a
        #    servi à choisir la signature — sinon on le déduit de la fiche client.
        interlocuteur = interlocuteur_fourni
        id_client = entier_texte(affaire.get("Client_ID"))
        if not interlocuteur and id_client:
            client = graph_get_item_by_id(site_id, clients_list_id, id_client, token)
            if client:
                interlocuteur = texte(client.get("Interlocuteur")) or texte(client.get("Interlocuteur_delegue"))

        # 6. Commune : Ville_realisation, puis Lieu_evt, puis nom de l'événement
        commune = texte(affaire.get("Ville_realisation")) or texte(affaire.get("Lieu_evt")) or nom_evt

        masse_active = nombre(devis.get("MA"))
        calibre_max = nombre(devis.get("Calibre_reference"))
        aujourd_hui = datetime.now(timezone.utc)

        valeurs = {
            CHAMP_PREFECTURE: prefecture,
            CHAMP_COMMUNE: commune,
            CHAMP_DATE_TIR: format_date(affaire.get("Date_evt")),
            CHAMP_MASSE_ACTIVE: format_masse_active(masse_active),
            CHAMP_CALIBRE_MAX: entier_texte(calibre_max),
            CHAMP_DECLARANT: interlocuteur,
            CHAMP_DATE_DECLARATION: aujourd_hui.strftime("%d/%m/%Y"),
            CASE_MASSE_SUP_35: ETAT_COCHE if (masse_active or 0) > SEUIL_MASSE_ACTIVE else ETAT_DECOCHE,
            CASE_MASSE_INF_35: ETAT_DECOCHE if (masse_active or 0) > SEUIL_MASSE_ACTIVE else ETAT_COCHE,
            CASE_NIVEAU_2: ETAT_COCHE if (calibre_max or 0) >= SEUIL_CALIBRE_NIVEAU_2 else ETAT_DECOCHE,
            CASE_NIVEAU_1: ETAT_DECOCHE if (calibre_max or 0) >= SEUIL_CALIBRE_NIVEAU_2 else ETAT_COCHE,
            CASE_SANS_NIVEAU: ETAT_DECOCHE,
        }

        # 7. Génération du PDF, puis apposition de la signature si elle a été transmise
        template_bytes = download_graph_file(site_id, token, f"{TEMPLATE_FOLDER}/{modele}")
        pdf_bytes = remplir_pdf(template_bytes, valeurs)

        signature_apposee = False
        if signature_base64:
            try:
                pdf_bytes, signature_apposee = apposer_signature(
                    pdf_bytes, base64.b64decode(signature_base64))
            except Exception as e:
                logging.error(f"Signature inexploitable, CERFA généré sans signature : {e}")
        else:
            logging.warning(f"Aucune signature transmise pour « {interlocuteur or 'interlocuteur inconnu'} » : "
                            "le CERFA devra être signé manuellement.")

        # 8. Dépôt du fichier et création de l'élément de liste
        annee = format_date(affaire.get("Date_evt"))[-4:] or str(aujourd_hui.year)
        libelle = f"CERFA 14098 - {annee}-{initiales(interlocuteur)}-{commune}".strip()
        libelle = re.sub(r'[\\/:*?"<>|]', " ", libelle).strip()
        nom_fichier = f"{libelle}_{aujourd_hui.strftime('%Y-%m-%d-%H-%M-%S')}.pdf"

        item_id = graph_post_item(site_id, affaire_doc_list_id, token, {
            "Title": libelle,
            "Aff_ID": id_affaire,
            "Type_doc": TYPE_DOC,
        })

        ensure_folder(site_id, token, TARGET_FOLDER)
        drive_item_id, file_list_item_id = deposer_fichier(
            site_id, token, nom_fichier, pdf_bytes, "application/pdf")

        # 9. Annexe « liste des produits », second document du dossier préfecture
        distance = nombre(affaire.get("Distance_securite")) or DISTANCE_PAR_DEFAUT
        if not nombre(affaire.get("Distance_securite")):
            logging.warning(f"Affaire {id_affaire} sans distance de sécurité : "
                            f"repli sur {DISTANCE_PAR_DEFAUT} m.")

        produits = graph_filtered_items(site_id, produits_list_id, token)
        retenus = selectionner_produits(produits, distance)
        logging.info(f"{len(retenus)} produits retenus sur {len(produits)} "
                     f"pour une distance de sécurité de {distance:.0f} m.")

        libelle_produits = re.sub(
            r'[\\/:*?"<>|]', " ",
            f"Produits - {annee}-{initiales(interlocuteur)}-{commune}").strip()
        nom_fichier_produits = f"{libelle_produits}_{aujourd_hui.strftime('%Y-%m-%d-%H-%M-%S')}.pdf"

        categories = ", ".join(sorted({texte(p.get("Cl")) for p in retenus if texte(p.get("Cl"))}))
        pdf_produits = construire_pdf_produits(
            retenus, format_masse_active(masse_active), categories)

        item_id_produits = graph_post_item(site_id, affaire_doc_list_id, token, {
            "Title": libelle_produits,
            "Aff_ID": id_affaire,
            "Type_doc": TYPE_DOC_PRODUITS,
        })
        drive_item_id_produits, file_list_item_id_produits = deposer_fichier(
            site_id, token, nom_fichier_produits, pdf_produits, "application/pdf")

        return func.HttpResponse(
            json.dumps({
                "status": "success",
                "message": f"Le CERFA de l'affaire {nom_evt or id_affaire} a été généré, accompagné de la liste des {len(retenus)} produits.",
                "created_item_id": item_id,
                "item_id": item_id,
                "Type_Doc": TYPE_DOC,
                "drive_item_id": drive_item_id,
                "file_list_item_id": file_list_item_id,
                "filename": nom_fichier,
                "folder": TARGET_FOLDER,
                "ID_evt": id_affaire,
                "nom_evt": nom_evt,
                "commune": commune,
                "interlocuteur": interlocuteur,
                "signature_apposee": "true" if signature_apposee else "false",
                "produits_item_id": item_id_produits,
                "produits_filename": nom_fichier_produits,
                "produits_drive_item_id": drive_item_id_produits,
                "produits_file_list_item_id": file_list_item_id_produits,
                "nb_produits": str(len(retenus)),
                "distance_securite": entier_texte(distance),
                "ID_devis": entier_texte(devis.get("id")),
                "devis_num": texte(devis.get("Title")),
                "entite": entite,
            }, ensure_ascii=False),
            status_code=200,
            mimetype="application/json"
        )

    except Exception as e:
        logging.exception("Erreur dans GenerationCerfa")
        return func.HttpResponse(
            json.dumps({"status": "error", "code": "erreur_interne",
                        "message": f"Erreur serveur interne : {str(e)}"}, ensure_ascii=False),
            status_code=500,
            mimetype="application/json"
        )
