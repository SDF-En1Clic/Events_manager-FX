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
    url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/items/{item_id}/fields"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    res = session.patch(url, headers=headers, json=updates)
    res.raise_for_status()

def graph_execute_batch(token, batch_requests):
    """Exécute un batch (max 20 requêtes) via Graph API pour être ultra-rapide"""
    url = "https://graph.microsoft.com/v1.0/$batch"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    payload = {"requests": batch_requests}
    res = session.post(url, headers=headers, json=payload)
    res.raise_for_status()
    return res.json()

def download_first_attachment(site_id, list_id, item_id, token):
    """Récupère le contenu binaire de la première pièce jointe de l'item"""
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
        
        # NOTE : Assure-toi que ces secrets existent dans ton KeyVault, 
        # ou remplace-les par les ID fixes si ce sont de nouvelles listes.
        commandes_list_id = get_secret("cmdlistid") 
        details_list_id = get_secret("cmddetailslistid")
        config_list_id = get_secret("configlistid") # Nouvelle liste de ton Flow

        token = get_graph_token(tenant_id, client_id, client_secret)
        if not token:
            return func.HttpResponse("Échec de l'authentification Graph", status_code=500)

        # --- 2. Récupérer la commande
        commande_item = graph_get_item_by_id(site_id, commandes_list_id, item_id, token)
        if not commande_item:
            return func.HttpResponse(f"Commande {item_id} introuvable", status_code=404)
        
        cmd_title = commande_item.get("fields", {}).get("Title")
        type_import = commande_item.get("fields", {}).get("Type_import")

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
            if len(delete_batch) == 20: # Limite Graph API = 20
                graph_execute_batch(token, delete_batch)
                delete_batch = []
        if delete_batch:
            graph_execute_batch(token, delete_batch)

        # --- 4. Télécharger la PJ
        logging.info("Téléchargement de la PJ...")
        file_content = download_first_attachment(site_id, commandes_list_id, item_id, token)
        if not file_content:
            return func.HttpResponse("Aucune pièce jointe trouvée", status_code=400)

        statut_import = "oui"
        nouveaux_details = [] # Liste pour préparer le batch d'insertion

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

            # Préparation des lignes
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
            # Header=None car on split via index comme dans Automate
            df_csv = pd.read_csv(io.BytesIO(file_content), sep=';', header=None, dtype=str).fillna("")
            
            for _, row in df_csv.iterrows():
                ref = str(row[0]).strip()
                if ref.lower() in ["rfrence", "référence", ""]: continue # Ignore header ou vide

                nouveaux_details.append({
                    "Title": str(row[2]),
                    "Reference": ref,
                    "Description_pyro": str(row[1]),
                    "Quantite": str(row[3]),
                    "Comptabilise_inventaire": 0,
                    "Statut": "Attente validation",
                    "CMD_ID": cmd_title
                })

        elif type_import == "Fichier pyromotion":
            # --- LECTURE CSV PYROMOTION ---
            df_csv = pd.read_csv(io.BytesIO(file_content), sep=';', header=None, dtype=str).fillna("")
            ligne_memoire = "0"
            for _, row in df_csv.iterrows():
                col0 = str(row[0])
                if "Ligne" in col0:
                    ligne_memoire = col0.split(' ')[1].strip() if len(col0.split(' ')) > 1 else "0"
                else:
                    ref = str(row[1]).strip()
                    if ref:
                        nouveaux_details.append({
                            "Title": ligne_memoire,
                            "Reference": ref,
                            "Quantite": str(row[3]).strip(),
                            "Statut": "Attente validation",
                            "CMD_ID": cmd_title
                        })

        elif type_import == "Fichier FWSIM":
            # --- LECTURE CSV FWSIM ---
            df_csv = pd.read_csv(io.BytesIO(file_content), sep=';', header=None, dtype=str).fillna("")
            
            # NOTE: Dans Power Automate tu créais un élément dans la liste "Déclenchement analyse pyro"
            # Si nécessaire, ajoute le post ici via graph_execute_batch ou un call direct.
            
            chain_id = ""
            module_v = ""
            pin_v = ""
            line_v = ""

            for _, row in df_csv.iterrows():
                col0 = str(row[0])
                if "Time" in col0 or col0 == "": continue

                current_chain = str(row[8]) if len(row) > 8 else ""
                
                if current_chain != chain_id and current_chain != "":
                    chain_id = current_chain
                    line_v = str(row[10]) if len(row) > 10 else ""
                    pin_v = str(row[6]) if len(row) > 6 else ""
                    module_v = str(row[5]) if len(row) > 5 else ""

                ref = str(row[1])
                qty = str(row[3]).strip()
                
                titre = f"{module_v}-{pin_v}" if not line_v else line_v
                if str(row[9]) == chain_id: # Condition Automate
                     titre = str(row[9])

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

        # --- 7. Mise à jour de la commande parente
        graph_update_field(site_id, commandes_list_id, item_id, token, {"StatutImport": statut_import})
        
        logging.info("Import terminé avec succès !")
        return func.HttpResponse(json.dumps({"status": "success", "lignes_creees": len(nouveaux_details)}), mimetype="application/json", status_code=200)

    except Exception as e:
        logging.exception("Erreur critique dans la fonction Azure Import")
        return func.HttpResponse(f"Erreur serveur : {str(e)}", status_code=500)