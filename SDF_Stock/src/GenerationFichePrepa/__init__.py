import logging
import azure.functions as func
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
import requests
import urllib.parse
import json
from datetime import datetime
import io
from openpyxl import load_workbook
from openpyxl.utils.cell import column_index_from_string, get_column_letter
import re

# --------- CONFIG GLOBALE -------------
VAULT_URL = "https://events-manager-kv.vault.azure.net/"
TEMPLATE_PATH = "00 - Templates/Template_fiche_prepa_V1.xlsx"
SP_HOST = "https://o365soirsdefetes.sharepoint.com"
SP_SITE_URL = f"{SP_HOST}/sites/Events_manager-Database"
TYPE_DOC = "Fiche préparation"
# --------------------------------------

session = requests.Session()
adapter = requests.adapters.HTTPAdapter(pool_connections=100, pool_maxsize=100)
session.mount('https://', adapter)


# =========================================================================
#  KEY VAULT / GRAPH HELPERS  (repris de GenerationDocument)
# =========================================================================
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


def get_sp_token(tenant_id, client_id, client_secret, sp_user, sp_pass):
    """Token DÉLÉGUÉ scopé SharePoint via le compte de service (ROPC).

    SharePoint Online refuse les tokens app-only obtenus avec un client secret ;
    on passe donc par le compte de service (spusername/sppassword), comme le
    connecteur SharePoint de l'automate.
    """
    if not sp_user or not sp_pass:
        logging.error("spusername/sppassword absents : impossible d'obtenir un token SharePoint.")
        return None
    url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
    data = {
        "grant_type": "password",
        "client_id": client_id,
        "client_secret": client_secret,
        "username": sp_user,
        "password": sp_pass,
        "scope": f"{SP_HOST}/.default",
    }
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    try:
        response = session.post(url, data=data, headers=headers)
        if not response.ok:
            logging.error(f"Erreur token SharePoint ({response.status_code}): {response.text}")
        response.raise_for_status()
        return response.json().get("access_token")
    except Exception as e:
        logging.error(f"Exception token SharePoint: {e}")
        return None


def get_form_digest(sp_token):
    """Récupère un X-RequestDigest pour les écritures REST SharePoint."""
    url = f"{SP_SITE_URL}/_api/contextinfo"
    headers = {"Authorization": f"Bearer {sp_token}", "Accept": "application/json;odata=verbose"}
    try:
        res = session.post(url, headers=headers)
        if res.ok:
            return res.json()["d"]["GetContextWebInformation"]["FormDigestValue"]
        logging.warning(f"contextinfo indisponible ({res.status_code}): {res.text}")
    except Exception as e:
        logging.warning(f"Exception contextinfo: {e}")
    return None


def add_attachment(sp_token, digest, list_id, item_id, filename, file_bytes):
    """Ajoute le fichier en pièce jointe d'un élément de liste (comme l'automate).

    Utilise l'API REST SharePoint car Microsoft Graph ne gère pas les pièces
    jointes des éléments de liste. Retourne (ok: bool, detail: str).
    """
    safe_name = filename.replace("'", "''")
    url = (f"{SP_SITE_URL}/_api/web/lists(guid'{list_id}')/items({item_id})"
           f"/AttachmentFiles/add(FileName='{urllib.parse.quote(safe_name)}')")
    headers = {
        "Authorization": f"Bearer {sp_token}",
        "Accept": "application/json;odata=verbose",
        "Content-Type": "application/octet-stream",
    }
    if digest:
        headers["X-RequestDigest"] = digest
    try:
        res = session.post(url, headers=headers, data=file_bytes)
        if not res.ok:
            logging.error(f"Erreur ajout pièce jointe ({res.status_code}): {res.text}")
            return False, f"{res.status_code}: {res.text[:300]}"
        return True, "ok"
    except Exception as e:
        logging.error(f"Exception ajout pièce jointe: {e}")
        return False, str(e)


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

    out = []
    for item in results:
        fields = item.get("fields", {})
        # On conserve l'id Graph de l'élément (= ID de liste) pour les PatchItem.
        fields.setdefault("_item_id", item.get("id"))
        out.append(fields)
    return out


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


def download_graph_file(site_id, token, filepath):
    url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives"
    headers = {"Authorization": f"Bearer {token}"}
    res = session.get(url, headers=headers)
    res.raise_for_status()
    drives = res.json().get("value", [])

    drive_id = None
    for d in drives:
        if d.get("name") in ("Documents partages", "Documents partagés"):
            drive_id = d.get("id")
            break
    if not drive_id and drives:
        drive_id = drives[0].get("id")

    url_file = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/root:/{urllib.parse.quote(filepath)}:/content"
    res_file = session.get(url_file, headers=headers)
    res_file.raise_for_status()
    return res_file.content


def graph_post_item(site_id, list_id, token, fields_dict):
    url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/items"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    res = session.post(url, headers=headers, json={"fields": fields_dict})
    if not res.ok:
        logging.error(f"Erreur create item: {res.status_code} {res.text}")
    res.raise_for_status()
    return res.json().get("id")


def graph_patch_item(site_id, list_id, item_id, token, fields_dict):
    """Met à jour les champs d'un élément de liste (PatchItem du flux)."""
    url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/items/{item_id}/fields"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    try:
        res = session.patch(url, headers=headers, json=fields_dict)
        if not res.ok:
            logging.error(f"Erreur patch item {item_id}: {res.status_code} {res.text}")
        return res.ok
    except Exception as e:
        logging.error(f"Exception patch item {item_id}: {e}")
        return False


# =========================================================================
#  EXCEL HELPERS
# =========================================================================
def append_to_table(sheet, table, data_list):
    """Ajoute des lignes à une table Excel (DATAS) et étend sa plage."""
    if not data_list:
        return

    match = re.search(r'([A-Za-z]+)(\d+):([A-Za-z]+)(\d+)', table.ref)
    if not match:
        return

    start_col, start_row, end_col, end_row = match.groups()
    start_row = int(start_row)
    start_col_idx = column_index_from_string(start_col)
    end_col_idx = column_index_from_string(end_col)

    headers = []
    for col_idx in range(start_col_idx, end_col_idx + 1):
        headers.append(sheet.cell(row=start_row, column=col_idx).value)

    current_row = start_row
    for row_data in data_list:
        current_row += 1
        for col_idx, header in enumerate(headers, start=start_col_idx):
            sheet.cell(row=current_row, column=col_idx, value=row_data.get(header, ""))

    table.ref = f"{start_col}{start_row}:{end_col}{current_row}"


def write_sheet_rows(ws, header_row, col_order, data_list):
    """Écrit des valeurs statiques dans un onglet de vue (Materiel / Accessoires).

    Les formules de "spill" déjà présentes sous l'en-tête sont écrasées par les
    valeurs ; les lignes excédentaires éventuelles sont nettoyées.
    """
    first_data_row = header_row + 1
    # On nettoie toute la bande de colonnes (des formules "spill" du template
    # subsistent au-delà des colonnes d'en-tête, ex. colonne CASE).
    n_cols = max(len(col_order), ws.max_column)
    rows_to_clear = max(len(data_list), ws.max_row - header_row)
    for i in range(rows_to_clear):
        r = first_data_row + i
        values = data_list[i] if i < len(data_list) else None
        for c_idx in range(1, n_cols + 1):
            key = col_order[c_idx - 1] if c_idx <= len(col_order) else None
            if values is not None and key is not None:
                ws.cell(row=r, column=c_idx).value = values.get(key, "")
            else:
                ws.cell(row=r, column=c_idx).value = None


def get_tables(wb):
    """Retourne un dict {nom_table: (table, sheet)} pour toutes les tables du classeur."""
    found = {}
    for sheet in wb.worksheets:
        for name in list(sheet.tables):
            found[name] = (sheet.tables[name], sheet)
    return found


# =========================================================================
#  UTILITAIRES MÉTIER
# =========================================================================
def clean_id(val):
    if val is None or val == "":
        return ""
    if isinstance(val, float):
        return str(int(val))
    return str(val)


def fmt_date(val):
    if not val:
        return ""
    try:
        return datetime.strptime(str(val)[:10], "%Y-%m-%d").strftime("%d/%m/%Y")
    except Exception:
        return val


def fmt_qte(val):
    """Quantité lisible (entier si possible)."""
    if val is None or val == "":
        return ""
    try:
        f = float(val)
        return int(f) if f.is_integer() else f
    except Exception:
        return val


def fetch_details(site_id, list_id, token, commande_id):
    """Détails commande, en gérant CMD_ID texte ou numérique (comme GenerationDocument)."""
    details = []
    try:
        details = graph_filtered_items(site_id, list_id, token, f"fields/CMD_ID eq '{commande_id}'")
    except Exception:
        pass
    if not details:
        try:
            details = graph_filtered_items(site_id, list_id, token, f"fields/CMD_ID eq {commande_id}")
        except Exception:
            pass
    return details


def tri_lignes(item):
    val = str(item.get("Title", "")).replace("Ligne", "").strip()
    try:
        return (0, float(val), str(item.get("Reference", "")).strip().upper())
    except ValueError:
        return (1, val, str(item.get("Reference", "")).strip().upper())


# =========================================================================
#  CONSTRUCTION DES DONNÉES D'UN FICHIER (pour un site de stock)
# =========================================================================
def build_fiche_prepa_rows(details_site, produits_sdf_map, inventaire_cache,
                           site_id, inventaire_list_id, details_list_id, token,
                           site_filter, is_secondary):
    """Construit les lignes de l'onglet principal (tabDatas) pour un site de stock
    et applique le PatchItem sur la liste Commandes_details.

    inventaire_cache : dict {(ref, scope): {"bat":, "empl":}} pour éviter les requêtes répétées.
    site_filter : valeur de Site_Stock_second utilisée pour cibler l'inventaire.
    """
    rows = []
    for det in details_site:
        ref = str(det.get("Reference", "")).strip().upper()
        prod = produits_sdf_map.get(ref)
        # Onglet principal = uniquement les produits d'origine SDF (filtre du flux)
        if not prod:
            continue

        # --- Emplacement / Bâtiment depuis l'inventaire ---
        cache_key = (ref, "sec" if is_secondary else "prim")
        if cache_key not in inventaire_cache:
            if is_secondary:
                inv_filter = f"fields/Title eq '{ref}' and fields/Site eq '{site_filter}'"
            else:
                inv_filter = f"fields/Title eq '{ref}' and fields/Site ne '{site_filter}'"
            bat, empl = "", ""
            try:
                inv_items = graph_filtered_items(site_id, inventaire_list_id, token, inv_filter)
                if inv_items:
                    bat = inv_items[0].get("Batiment", "") or ""
                    empl = inv_items[0].get("Emplacement", "") or ""
            except Exception as e:
                logging.warning(f"Inventaire introuvable pour {ref}: {e}")
            inventaire_cache[cache_key] = {"bat": bat, "empl": empl}

        inv = inventaire_cache[cache_key]
        bat_prefix = "P" if det.get("Emplacement") else ""

        rows.append({
            "LIGNE": f"Ligne {det.get('Title', '')}",
            "REFERENCE": ref,
            "DESIGNATION": prod.get("Description_pyromotion", ""),
            "QT": fmt_qte(det.get("Quantite", "")),
            "COMMENTAIRE": det.get("Commentaires", ""),
            "SITE": det.get("Site", ""),
            "EMPLACEMENT": f"{bat_prefix}{inv['empl']}",
            "STATUT": det.get("Statut", ""),
            "ORIGINE": prod.get("Origine", ""),
            "BATIMENT": inv["bat"],
            "CASE": "" if is_secondary else "☐",
        })

        # --- PatchItem -> Commandes_details (PAS l'inventaire) ---
        det_id = det.get("_item_id") or det.get("ID") or det.get("id")
        if det_id:
            graph_patch_item(site_id, details_list_id, det_id, token, {
                "Emplacement": inv["empl"],
                "Batiment": inv["bat"],
                "Comptabilise_inventaire": 0,
            })

    # --- Script 1 : tri BATIMENT, EMPLACEMENT, REFERENCE ---
    rows.sort(key=lambda r: (str(r.get("BATIMENT", "")), str(r.get("EMPLACEMENT", "")), str(r.get("REFERENCE", ""))))
    return rows


def build_grouped_rows(tab_datas_rows):
    """Script 2 : regroupe tabDatas par REFERENCE -> lignes de tabout (onglet Classés par produit)."""
    grouped = []
    prev_ref = None
    for r in tab_datas_rows:
        cur_ref = r.get("REFERENCE", "")
        if cur_ref != prev_ref:
            grouped.append({"REFERENCE": cur_ref})  # ligne d'en-tête de groupe
        grouped.append({
            "LIGNE": r.get("LIGNE", ""),
            "REFERENCE": "",
            "DESIGNATION": r.get("DESIGNATION", ""),
            "QT": r.get("QT", ""),
            "SITE": r.get("COMMENTAIRE", ""),  # mapping fidèle au script d'origine
            "EMPLACEMENT": r.get("EMPLACEMENT", ""),
            "STATUT": r.get("STATUT", ""),
            "ORIGINE": r.get("ORIGINE", ""),
            "BATIMENT": r.get("BATIMENT", ""),
        })
        prev_ref = cur_ref
    return grouped


def build_accessoires_rows(details_site, accessoires_map):
    """Onglet Accessoires : détails de la commande dont la référence est un accessoire."""
    rows = []
    for det in details_site:
        ref = str(det.get("Reference", "")).strip().upper()
        prod = accessoires_map.get(ref)
        if not prod:
            continue
        if not prod.get("Description_pyromotion"):
            continue
        rows.append({
            "REFERENCE": ref,
            "DESIGNATION": prod.get("Description_pyromotion", ""),
            "ORIGINE": prod.get("Origine", ""),
            "QTE": fmt_qte(det.get("Quantite", "")),
            "STATUT": det.get("Statut", ""),
            "BATIMENT": det.get("Batiment", ""),
            "EMPLACEMENT": det.get("Emplacement", ""),
        })
    rows.sort(key=lambda r: str(r.get("REFERENCE", "")))
    return rows


def build_materiel_rows(reservations_site, materiel_map):
    """Onglet Materiel : réservations (par Aff_ID) jointes à la liste Materiel.

    La jointure se fait sur le Materiel_ID qui correspond au champ interne
    `Title` dans les deux listes (affiché « Materiel_ID » dans SharePoint).
    Batiment/Emplacement viennent de la liste Materiel (absents de la réservation).
    """
    rows = []
    for resa in reservations_site:
        mat_id = clean_id(resa.get("Title"))
        mat = materiel_map.get(mat_id, {})
        reference = mat_id
        rows.append({
            "REFERENCE": reference,
            "DESIGNATION": mat.get("Designation", ""),
            "CATEGORIE": mat.get("Categorie", ""),
            "QTE": fmt_qte(resa.get("Quantite", "")),
            "STATUT": resa.get("Statut", "") or mat.get("Statut", ""),
            "BATIMENT": resa.get("Batiment", "") or mat.get("Batiment", ""),
            "EMPLACEMENT": resa.get("Emplacement", "") or mat.get("Emplacement", ""),
        })
    rows.sort(key=lambda r: str(r.get("REFERENCE", "")))
    return rows


def fill_workbook(template_bytes, header_data, fiche_rows, grouped_rows, accessoires_rows, materiel_rows):
    """Remplit toutes les feuilles du template pour un fichier (un site de stock)."""
    wb = load_workbook(io.BytesIO(template_bytes))
    tables = get_tables(wb)

    # En-tête (tabDataL) + lignes produits (tabDatas) + regroupement (tabout)
    if "tabDataL" in tables:
        tb, sh = tables["tabDataL"]
        append_to_table(sh, tb, [header_data])
    if "tabDatas" in tables:
        tb, sh = tables["tabDatas"]
        append_to_table(sh, tb, fiche_rows)
    if "tabout" in tables:
        tb, sh = tables["tabout"]
        append_to_table(sh, tb, grouped_rows)

    # Onglets Accessoires / Materiel : valeurs statiques (les formules de spill du
    # template pointent toutes vers tabDatas et sont incorrectes -> on les écrase).
    if "Accessoires" in wb.sheetnames:
        write_sheet_rows(
            wb["Accessoires"], 12,
            ["REFERENCE", "DESIGNATION", "ORIGINE", "QTE", "STATUT", "BATIMENT", "EMPLACEMENT"],
            accessoires_rows)
    if "Materiel" in wb.sheetnames:
        write_sheet_rows(
            wb["Materiel"], 12,
            ["REFERENCE", "DESIGNATION", "CATEGORIE", "QTE", "STATUT", "BATIMENT", "EMPLACEMENT"],
            materiel_rows)

    out = io.BytesIO()
    wb.save(out)
    return out.getvalue()


# =========================================================================
#  POINT D'ENTRÉE
# =========================================================================
def main(req: func.HttpRequest) -> func.HttpResponse:
    try:
        body = req.get_json()
        commande_id = body.get("ID_cmd")
        type_doc = body.get("Type_Doc", TYPE_DOC)

        logging.info(f"Paramètres reçus : ID_cmd={commande_id}, Type_Doc={type_doc}")
        if not commande_id:
            return _err("Paramètre 'ID_cmd' requis", 400)
        if type_doc != TYPE_DOC:
            return _err(f"Type_Doc non supporté: {type_doc}", 400)

        # --- Secrets ---
        tenant_id = get_secret("tenantid")
        client_id = get_secret("clientid")
        client_secret = get_secret("appsecret")
        site_id = get_secret("siteid")

        commandes_list_id = get_secret("cmdlistid")
        details_list_id = get_secret("cmddetailslistid")
        produits_list_id = get_secret("produitslistid")
        admin_list_id = get_secret("configlistid")
        affaire_list_id = get_secret("affaireevtslistid")
        clients_list_id = get_secret("clientslistid")
        commande_doc_list_id = get_secret("commandedoclistid")
        inventaire_list_id = get_secret("inventairelistid")
        materiel_list_id = get_secret("materiellistid")
        materiel_resa_list_id = get_secret("materielreservationlistid")

        token = get_graph_token(tenant_id, client_id, client_secret)
        if not token:
            return _err("Échec de l'authentification Graph", 500)

        # Token SharePoint DÉLÉGUÉ (compte de service) pour les pièces jointes
        sp_user = get_secret("spusername")
        sp_pass = get_secret("sppassword")
        sp_token = get_sp_token(tenant_id, client_id, client_secret, sp_user, sp_pass)
        sp_digest = get_form_digest(sp_token) if sp_token else None
        if not sp_token:
            logging.warning("Token SharePoint indisponible : les pièces jointes seront ignorées.")

        # --- 1. Commande ---
        commande = graph_get_item_by_id(site_id, commandes_list_id, commande_id, token)
        if not commande:
            return _err("Commande introuvable", 404)

        entite = commande.get("Entite", "")
        numero_commande = commande.get("Title", "")
        site_livraison_title = commande.get("Site_livraison", "")
        date_livraison = fmt_date(commande.get("Date_livraison", ""))
        site_stock = commande.get("Site_Stock", "")
        site_stock_second = commande.get("Site_Stock_second", "")

        # --- 2. Affaire / Client ---
        id_aff = clean_id(commande.get("Aff_ID"))
        client_name = ""
        date_tir = ""
        id_client = ""
        if id_aff:
            affaire = graph_get_item_by_id(site_id, affaire_list_id, id_aff, token)
            if affaire:
                date_tir = fmt_date(affaire.get("Date_evt", ""))
                id_client = clean_id(affaire.get("Client_ID"))
                if id_client:
                    cli = graph_get_item_by_id(site_id, clients_list_id, id_client, token)
                    if cli:
                        client_name = cli.get("Title", "").replace("'", " ")

        # --- 3. Émetteur (fournisseur de l'entité) ---
        emetteur = ""
        try:
            fournisseurs = graph_filtered_items(
                site_id, admin_list_id, token,
                f"fields/Clef eq 'Fournisseur' and fields/Entite eq '{entite}'")
            if fournisseurs:
                f0 = fournisseurs[0]
                emetteur = (f"{f0.get('Title','')}\n{f0.get('Param_x00e8_tres','')}\n"
                            f"{f0.get('Parametres_4','')} {f0.get('Parametres_3','')}")
        except Exception as e:
            logging.warning(f"Fournisseur introuvable: {e}")

        # --- 4. Site de livraison ---
        ville_livraison = adresse_livraison = telephone = ""
        try:
            sites = graph_filtered_items(
                site_id, admin_list_id, token,
                f"fields/Clef eq 'Site_livraison' and fields/Title eq '{site_livraison_title}' and fields/Entite eq '{entite}'")
            if sites:
                s0 = sites[0]
                ville_livraison = f"{s0.get('Parametres_2','')} - {s0.get('Parametres_3','')}"
                adresse_livraison = s0.get("Param_x00e8_tres", "")
                telephone = s0.get("Parametres_4", "")
        except Exception as e:
            logging.warning(f"Site de livraison introuvable: {e}")

        # --- 5. Détails commande ---
        details = fetch_details(site_id, details_list_id, token, commande_id)
        logging.info(f"{len(details)} lignes de détail trouvées.")

        # --- 6. Produits : map SDF (onglet principal) + map accessoires ---
        produits_sdf = graph_filtered_items(site_id, produits_list_id, token, "fields/Origine eq 'SDF'")
        produits_sdf_map = {}
        for p in produits_sdf:
            key = str(p.get("Title", "")).strip().upper()
            if key:
                produits_sdf_map[key] = p

        accessoires = graph_filtered_items(site_id, produits_list_id, token, "fields/Famille_FWSIM eq 'ACCESSOIRE'")
        accessoires_map = {}
        for p in accessoires:
            if str(p.get("Sous_famille_FWSIM", "")).strip().upper() == "UKOBA":
                continue
            key = str(p.get("Title", "")).strip().upper()
            if key:
                accessoires_map[key] = p

        # --- 7. Matériel : réservations (par Aff_ID) + liste matériel (par Materiel_ID) ---
        reservations = []
        if id_aff:
            reservations = fetch_by_field(site_id, materiel_resa_list_id, token, "Aff_ID", id_aff)
        materiel_items = graph_filtered_items(site_id, materiel_list_id, token)
        materiel_map = {}
        for m in materiel_items:
            # Materiel_ID = champ interne `Title` (affiché « Materiel_ID »)
            key = clean_id(m.get("Title"))
            if key:
                materiel_map[key] = m

        # --- 8. Génération : un fichier par site de stock ---
        # Nettoyage des anciens éléments commande_doc (ID_cmd + Type_Doc)
        cleanup_old_docs(site_id, commande_doc_list_id, token, commande_id, type_doc)

        timestamp = datetime.utcnow().strftime('%Y-%m-%d-%H-%M-%S')
        template_bytes = download_graph_file(site_id, token, TEMPLATE_PATH)

        # Définition des sites de stock à générer
        stock_targets = []
        if site_stock:
            stock_targets.append({"name": site_stock, "is_secondary": False})
        if site_stock_second:
            stock_targets.append({"name": site_stock_second, "is_secondary": True})
        if not stock_targets:
            # Aucun site de stock défini : on génère tout de même un fichier "principal"
            stock_targets.append({"name": "", "is_secondary": False})

        inventaire_cache = {}
        generated = []

        for target in stock_targets:
            is_sec = target["is_secondary"]
            dest_name = target["name"]
            # Valeur utilisée pour cibler l'inventaire / filtrer les lignes
            site_filter = site_stock_second if site_stock_second else "sec"

            # Filtrage des détails selon le site de stock
            if is_sec:
                details_site = [d for d in details if str(d.get("Site", "")) == str(site_stock_second)]
            else:
                details_site = [d for d in details if str(d.get("Site", "")) != str(site_filter)]

            # Réservations matériel filtrées par site
            if is_sec:
                resa_site = [r for r in reservations if str(r.get("Site", "")) == str(site_stock_second)]
            else:
                resa_site = [r for r in reservations if str(r.get("Site", "")) != str(site_filter)]

            header_data = {
                "Emetteur": emetteur,
                "Destinataire": dest_name,
                "Commande N°": numero_commande,
                "Client": client_name,
                "Adresse Livraison": adresse_livraison,
                "Ville Livraison": ville_livraison,
                "Telephone": telephone,
                "Site de livraison": site_livraison_title,
                "Date de livraison": date_livraison,
                "Date de tir": date_tir,
                "CREATEUR": "",
            }

            fiche_rows = build_fiche_prepa_rows(
                details_site, produits_sdf_map, inventaire_cache,
                site_id, inventaire_list_id, details_list_id, token,
                site_filter, is_sec)
            grouped_rows = build_grouped_rows(fiche_rows)
            accessoires_rows = build_accessoires_rows(details_site, accessoires_map)
            materiel_rows = build_materiel_rows(resa_site, materiel_map)

            file_bytes = fill_workbook(template_bytes, header_data, fiche_rows,
                                       grouped_rows, accessoires_rows, materiel_rows)

            # Nom de fichier
            suffix = f"_{dest_name}" if dest_name else ""
            nom_fichier = f"FP-{numero_commande}-{client_name}{suffix}_{timestamp}.xlsx"
            nom_fichier_doc = f"FP-{numero_commande}-{client_name}{suffix}"

            # Élément commande_doc
            item_id = graph_post_item(site_id, commande_doc_list_id, token, {
                "Title": nom_fichier_doc,
                "ID_cmd": str(commande_id),
                "ID_Aff": str(id_aff) if id_aff else "",
                "ID_Client": str(id_client) if id_client else "",
                "Type_Doc": type_doc,
            })

            # Pièce jointe sur l'élément de liste (comme l'automate)
            attached = False
            attach_detail = "token SharePoint indisponible" if not sp_token else "élément non créé"
            if sp_token and item_id:
                attached, attach_detail = add_attachment(
                    sp_token, sp_digest, commande_doc_list_id, item_id, nom_fichier, file_bytes)

            generated.append({
                "site_stock": dest_name,
                "filename": nom_fichier,
                "created_item_id": item_id,
                "attached": attached,
                "attach_detail": attach_detail,
                "nb_lignes": len(fiche_rows),
                "nb_accessoires": len(accessoires_rows),
                "nb_materiel": len(materiel_rows),
            })

        return func.HttpResponse(
            json.dumps({
                "status": "success",
                "message": f"{len(generated)} fiche(s) de préparation générée(s).",
                "Type_Doc": type_doc,
                "files": generated,
            }),
            status_code=200, mimetype="application/json")

    except Exception as e:
        logging.exception("Erreur dans GenerationFichePrepa")
        return _err(f"Erreur serveur interne : {str(e)}", 500)


# =========================================================================
#  HELPERS POINT D'ENTRÉE
# =========================================================================
def _err(message, code):
    return func.HttpResponse(
        json.dumps({"status": "error", "message": message}),
        status_code=code, mimetype="application/json")


def fetch_by_field(site_id, list_id, token, field, value):
    """Récupère des éléments par un champ donné (texte puis nombre)."""
    items = []
    try:
        items = graph_filtered_items(site_id, list_id, token, f"fields/{field} eq '{value}'")
    except Exception:
        pass
    if not items:
        try:
            items = graph_filtered_items(site_id, list_id, token, f"fields/{field} eq {value}")
        except Exception:
            pass
    return items


def cleanup_old_docs(site_id, list_id, token, commande_id, type_doc):
    try:
        headers = {"Authorization": f"Bearer {token}", "Prefer": "HonorNonIndexedQueriesWarningMayFailRandomly"}
        for filt in (f"fields/ID_cmd eq '{commande_id}' and fields/Type_Doc eq '{type_doc}'",
                     f"fields/ID_cmd eq {commande_id} and fields/Type_Doc eq '{type_doc}'"):
            fp = urllib.parse.quote(filt, safe="=()/ '")
            url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/items?$expand=fields&$filter={fp}"
            res = session.get(url, headers=headers)
            if res.status_code == 400:
                continue
            if res.ok:
                for old in res.json().get("value", []):
                    oid = old.get("id")
                    if oid:
                        session.delete(f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/items/{oid}",
                                       headers={"Authorization": f"Bearer {token}"})
                        logging.info(f"Ancien document supprimé: {oid}")
                break
    except Exception as e:
        logging.error(f"Erreur nettoyage anciens documents: {e}")
