import logging
import azure.functions as func
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
import requests
import urllib.parse
import json
import pandas as pd
import io

# --------- CONFIG GLOBALE -------------
VAULT_URL = "https://events-manager-kv.vault.azure.net/"
# --------------------------------------

# --- OPTIMISATION CRITIQUE : SESSION PERSISTANTE ---
session = requests.Session()
adapter = requests.adapters.HTTPAdapter(pool_connections=100, pool_maxsize=100)
session.mount('https://', adapter)
# ---------------------------------------------------

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

def graph_get_item_by_id(site_id, list_id, item_id, token):
    url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/items/{item_id}?$expand=fields"
    headers = {"Authorization": f"Bearer {token}"}
    res = session.get(url, headers=headers)
    if res.ok: return res.json()
    return None

def graph_filtered_items(site_id, list_id, token, filter_expr=None):
    base_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/items?$expand=fields"
    headers = {"Authorization": f"Bearer {token}", "Prefer": "HonorNonIndexedQueriesWarningMayFailRandomly"}
    if filter_expr:
        filter_param = urllib.parse.quote(filter_expr, safe="=()/ ")
        base_url += f"&$filter={filter_param}"
    
    results = []
    url = base_url
    while url:
        res = session.get(url, headers=headers)
        res.raise_for_status()
        data = res.json()
        results.extend(data.get("value", []))
        url = data.get("@odata.nextLink")
    return results

def graph_update_field(site_id, list_id, item_id, token, updates: dict):
    # L'API Graph attend le payload directement sur l'endpoint /fields
    url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/items/{item_id}/fields"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    res = session.patch(url, headers=headers, json=updates)
    res.raise_for_status()

def graph_execute_batch(token, batch_requests):
    """Exécute un batch (max 20 requêtes) via Graph API"""
    url = "https://graph.microsoft.com/v1.0/$batch"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    payload = {"requests": batch_requests}
    res = session.post(url, headers=headers, json=payload)
    res.raise_for_status()
    return res.json()

def download_first_attachment(site_id, list_id, item_id, token):
    """Récupère le contenu binaire de la 1ère pièce jointe de l'élément SharePoint"""
    url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/items/{item_id}/driveItem"
    headers = {"Authorization": f"Bearer {token}"}
    res = session.get(url, headers=headers)
    if not res.ok: return None
    drive_item = res.json()
    
    download_url = drive_item.get("@microsoft.graph.downloadUrl")
    if download_url:
        content_res = session.get(download_url)
        return content_res.content
    return None

# ==============================================================================
# FONCTION PRINCIPALE
# ==============================================================================
def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Début du traitement Import Commande.')

    try:
        req_body = req.get_json()
        item_id = req_body.get('number')
        bypass = req_body.get('text', '').lower()
    except ValueError:
        return func.HttpResponse("Invalid JSON", status_code=400)

    if not item_id:
        return func.HttpResponse("L'ID (number) est requis.", status_code=400)

    try:
        # --- 1. Init Secrets & Token
        tenant_id = get_secret("tenantid")
        client_id = get_secret("clientid")
        client_secret = get_secret("appsecret")
        site_id = get_secret("siteid")
        
        # Mapping strict des listes (remplace ces valeurs en dur si pas dans KeyVault)
        import_list_id = get_secret("importlistid") or "6b2e67e2-1804-4f67-ba70-a25351ec8da1"
        details_list_id = get_secret("cmddetailslistid") or "d24aeeb6-47a8-415b-ad6f-186bdfde2a2f"
        config_list_id = get_secret("configlistid") or "02dd96b5-f1e2-4c13-ab77-f63dbf045743"

        token = get_graph_token(tenant_id, client_id, client_secret)
        if not token:
            return func.HttpResponse("Échec de l'authentification Graph", status_code=500)

        # --- 2. Récupérer l'élément d'import
        import_item = graph_get_item_by_id(site_id, import_list_id, item_id, token)
        if not import_item:
            return func.HttpResponse(f"Élément d'import {item_id} introuvable", status_code=404)
        
        cmd_title = import_item.get("fields", {}).get("Title")
        type_import = import_item.get("fields", {}).get("Type_import")

        if not type_import:
            return func.HttpResponse("Type_import non défini sur la commande.", status_code=400)

        # --- 3. Purger les anciens détails (Batch Delete)
        logging.info(f"Purge des anciens détails pour CMD_ID: {cmd_title}")
        old_items = graph_filtered_items(site_id, details_list_id, token, f"fields/CMD_ID eq '{cmd_title}'")
        
        delete_batch = []
        for index, item in enumerate(old_items):
            delete_batch.append({
                "id": str(index + 1),
                "method": "DELETE",
                "url": f"/sites/{site_id}/lists/{details_list_id}/items/{item['id']}"
            })
            if len(delete_batch) == 20: 
                graph_execute_batch(token, delete_batch)
                delete_batch = []
        if delete_batch:
            graph_execute_batch(token, delete_batch)

        # --- 4. Télécharger la PJ
        logging.info("Téléchargement de la PJ...")
        file_content = download_first_attachment(site_id, import_list_id, item_id, token)
        if not file_content:
            return func.HttpResponse("Aucune pièce jointe trouvée", status_code=400)

        statut_import = "oui"
        nouveaux_details = []

        # --- 5. COMMUTATEUR (SWITCH)
        if type_import in ["Fichier prestation", "Fichier grossiste"]:
            # --- LECTURE EXCEL ---
            df_total = pd.read_excel(io.BytesIO(file_content), sheet_name='TabTotal')
            df_datas = pd.read_excel(io.BytesIO(file_content), sheet_name='TabDatas')
            
            total_prix_excel = str(df_total.iloc[0].get('TotalPrix', ''))

            # Vérif BDD Config
            clef_config = 'TotalPrixTarifsPresta' if type_import == "Fichier prestation" else 'TotalPrixTarifsGrossiste'
            config_items = graph_filtered_items(site_id, config_list_id, token, f"fields/Clef eq '{clef_config}'")
            total_prix_bdd = config_items[0].get("fields", {}).get("Title", "") if config_items else ""

            if bypass != "ok" and total_prix_excel != total_prix_bdd:
                statut_import = "maj"

            df_datas = df_datas.dropna(subset=['Ligne'])
            for _, row in df_datas.iterrows():
                ligne_val = str(row.get('Ligne', ''))
                titre = ligne_val.split(' ')[1].strip() if len(ligne_val.split(' ')) > 1 else ligne_val
                
                nouveaux_details.append({
                    "Title": titre,
                    "Reference": str(row.get('Référence', '')),
                    "Quantite": str(row.get('Quantité', '')),
                    "Statut": "Attente validation",
                    "CMD_ID": cmd_title
                })

        elif type_import == "Fichier Finale3D":
            # --- LECTURE CSV FINALE3D ---
            try:
                csv_text = file_content.decode('utf-8') 
            except UnicodeDecodeError:
                csv_text = file_content.decode('latin-1')
            df_csv = pd.read_csv(io.StringIO(csv_text), sep=';', header=None, dtype=str).fillna("")
            
            for _, row in df_csv.iterrows():
                ref = str(row.get(0, "")).strip()
                if ref.lower() in ["rfrence", "référence", ""]: continue

                nouveaux_details.append({
                    "Title": str(row.get(2, "")),
                    "Reference": ref,
                    "Description_pyro": str(row.get(1, "")),
                    "Quantite": str(row.get(3, "")),
                    "Comptabilise_inventaire": 0,
                    "Statut": "Attente validation",
                    "CMD_ID": cmd_title
                })

        elif type_import == "Fichier pyromotion":
            # --- LECTURE CSV PYROMOTION ---
            try:
                csv_text = file_content.decode('utf-8') 
            except UnicodeDecodeError:
                csv_text = file_content.decode('latin-1')
            df_csv = pd.read_csv(io.StringIO(csv_text), sep=';', header=None, dtype=str).fillna("")
            
            ligne_memoire = "0"
            for _, row in df_csv.iterrows():
                col0 = str(row.get(0, ""))
                if "Ligne" in col0:
                    ligne_memoire = col0.split(' ')[1].strip() if len(col0.split(' ')) > 1 else "0"
                else:
                    ref = str(row.get(1, "")).strip()
                    if ref:
                        nouveaux_details.append({
                            "Title": ligne_memoire,
                            "Reference": ref,
                            "Quantite": str(row.get(3, "")).strip(),
                            "Statut": "Attente validation",
                            "CMD_ID": cmd_title
                        })

        elif type_import == "Fichier FWSIM":
            # --- LECTURE CSV FWSIM ---
            try:
                csv_text = file_content.decode('utf-8') 
            except UnicodeDecodeError:
                csv_text = file_content.decode('latin-1')
            df_csv = pd.read_csv(io.StringIO(csv_text), sep=';', header=None, dtype=str).fillna("")
            
            chain_id = ""
            module_v = ""
            pin_v = ""
            line_v = ""

            for _, row in df_csv.iterrows():
                            col0 = str(row.get(0, "")).strip() 
                            if "Time" in col0 or col0 == "": 
                                continue

                            current_chain = str(row.get(8, "")).strip()
                            if current_chain != chain_id and current_chain != "":
                                chain_id = current_chain
                                module_v = str(row.get(5, "")).strip()
                                pin_v = str(row.get(6, "")).strip()   
                                line_v = str(row.get(10, "")).strip() 

                            ref = str(row.get(1, "")).strip()
                            qty = str(row.get(3, "")).strip()
                            col9 = str(row.get(9, "")).strip()

                            # ==========================================
                            # NOUVELLE CONDITION : Ignorer si "MAT"
                            # ==========================================
                            if "Mat" in col9:  # (ou col9 == "MAT" si ça doit être exact)
                                continue
                            # ==========================================

                            if col9 == chain_id:
                                titre = col9
                            elif line_v == "" or line_v == "nan": 
                                titre = f"{module_v}-{pin_v}"
                            else:
                                titre = line_v

                            nouveaux_details.append({
                                "Title": titre,
                                "Reference": ref,
                                "Quantite": qty,
                                "Statut": "Attente validation",
                                "CMD_ID": cmd_title
                            })
        else:
            return func.HttpResponse(f"Type import inconnu: {type_import}", status_code=400)


        # --- 6. Insertion Massive (Batch Post)
        logging.info(f"Création de {len(nouveaux_details)} nouvelles lignes...")
        post_batch = []
        for index, item_payload in enumerate(nouveaux_details):
            post_batch.append({
                "id": str(index + 1),
                "method": "POST",
                "url": f"/sites/{site_id}/lists/{details_list_id}/items",
                "body": {"fields": item_payload},
                "headers": {"Content-Type": "application/json"}
            })
            if len(post_batch) == 20:
                graph_execute_batch(token, post_batch)
                post_batch = []
        if post_batch:
            graph_execute_batch(token, post_batch)

        # --- 7. Mise à jour de l'élément d'import
        # On met à jour l'élément depuis lequel le flux a été lancé (import_list_id)
        graph_update_field(site_id, import_list_id, item_id, token, {"StatutImport": statut_import})
        
        logging.info("Import terminé avec succès !")
        return func.HttpResponse(json.dumps({"status": "success", "lignes_creees": len(nouveaux_details)}), mimetype="application/json", status_code=200)

    except Exception as e:
        logging.exception("Erreur critique dans la fonction Azure Import")
        return func.HttpResponse(f"Erreur serveur : {str(e)}", status_code=500)