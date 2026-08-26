import logging
import azure.functions as func
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
import requests
import urllib.parse
import json
import io
import re
from datetime import datetime, timezone

from pypdf import PdfReader, PdfWriter
from pypdf.generic import NameObject, TextStringObject

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

CASE_MASSE_SUP_35 = "Check Box10"
CASE_MASSE_INF_35 = "Check Box11"
CASE_NIVEAU_1 = "Check Box15"
CASE_NIVEAU_2 = "Check Box16"
CASE_SANS_NIVEAU = "Check Box17"

ETAT_COCHE = "Oui"
ETAT_DECOCHE = "Off"

SEUIL_MASSE_ACTIVE = 35      # kg : au-delà, la case "plus de 35 kg" est cochée
SEUIL_CALIBRE_NIVEAU_2 = 100  # mm : à partir de ce calibre, niveau 2 au lieu de niveau 1
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

        logging.info(f"Paramètres reçus : ID_evt={id_affaire}, ID_devis={id_devis_force}")
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

        # 5. Interlocuteur SdF, porté par la fiche client
        interlocuteur = ""
        id_client = entier_texte(affaire.get("Client_ID"))
        if id_client:
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

        # 7. Génération du PDF
        template_bytes = download_graph_file(site_id, token, f"{TEMPLATE_FOLDER}/{modele}")
        pdf_bytes = remplir_pdf(template_bytes, valeurs)

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
        url_upload = (f"https://graph.microsoft.com/v1.0/sites/{site_id}/drive/root:/"
                      f"{TARGET_FOLDER}/{urllib.parse.quote(nom_fichier, safe='')}:/content")
        res_upload = session.put(url_upload, headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/pdf"
        }, data=pdf_bytes)
        res_upload.raise_for_status()
        drive_item_id = res_upload.json().get("id", "")

        file_list_item_id = ""
        try:
            url_list_item = (f"https://graph.microsoft.com/v1.0/sites/{site_id}"
                             f"/drive/items/{drive_item_id}?$expand=listItem")
            res_li = session.get(url_list_item, headers={"Authorization": f"Bearer {token}"})
            if res_li.ok:
                file_list_item_id = res_li.json().get("listItem", {}).get("id", "")
        except Exception as e:
            logging.warning(f"Impossible de récupérer le listItemId du fichier : {e}")

        return func.HttpResponse(
            json.dumps({
                "status": "success",
                "message": f"Le CERFA de l'affaire {nom_evt or id_affaire} a été généré.",
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
