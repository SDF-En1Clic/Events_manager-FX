import logging
import azure.functions as func
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
import requests
import urllib.parse
import json
import pandas as pd
import openpyxl
import io
import base64

# --------- CONFIG GLOBALE -------------
VAULT_URL = "https://events-manager-kv.vault.azure.net/"
# --------------------------------------

# --- OPTIMISATION CRITIQUE : SESSION PERSISTANTE ---
session = requests.Session()
adapter = requests.adapters.HTTPAdapter(pool_connections=100, pool_maxsize=100)
session.mount('https://', adapter)
# ---------------------------------------------------

def json_response(data: dict, status_code: int = 200) -> func.HttpResponse:
    """Utilitaire pour renvoyer des réponses JSON standardisées."""
    return func.HttpResponse(
        json.dumps(data),
        mimetype="application/json",
        status_code=status_code
    )

def get_secret(name: str):
    credential = DefaultAzureCredential()
    client = SecretClient(vault_url=VAULT_URL, credential=credential)
    return client.get_secret(name).value

def get_excel_table_as_df(file_bytes, table_name):
    """
    Cherche un 'Tableau' (ListObject) Excel par son nom dans tout le classeur,
    extrait ses données et renvoie un DataFrame Pandas.
    """
    wb = openpyxl.load_workbook(io.BytesIO(file_bytes), data_only=True)
    
    for sheet in wb.worksheets:
        if table_name in sheet.tables:
            table = sheet.tables[table_name]
            table_range = sheet[table.ref]
            
            data = [[cell.value for cell in row] for row in table_range]
            
            if not data:
                return pd.DataFrame()
                
            columns = data[0]
            rows = data[1:]
            return pd.DataFrame(rows, columns=columns)
            
    raise ValueError(f"Le tableau Excel '{table_name}' est introuvable dans le fichier.")

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
    url = "https://graph.microsoft.com/v1.0/$batch"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    payload = {"requests": batch_requests}
    res = session.post(url, headers=headers, json=payload)
    res.raise_for_status()
    return res.json()

# ==============================================================================
# FONCTION PRINCIPALE 
# ==============================================================================
def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Début du traitement Import Commande.')
    
    # Variables globales pour le retour d'erreur 
    cmd_title = "Inconnu"
    type_import = "Inconnu"

    try:
        req_body = req.get_json()
        item_id = req_body.get('number')
        bypass = req_body.get('text', '').lower()
        file_b64 = req_body.get('file_base64')
    except ValueError:
        return json_response({"status": "error", "message": "Invalid JSON reçu par l'API."}, 400)

    if not item_id:
        return json_response({"status": "error", "message": "L'ID (number) est requis."}, 400)
        
    if not file_b64:
        return json_response({"status": "error", "message": "Aucun fichier (file_base64) n'a été transmis.", "cmd_id": item_id}, 400)

    try:
        logging.info("Décodage de la pièce jointe reçue depuis Power Automate...")
        if "base64," in file_b64:
            file_b64 = file_b64.split("base64,")[-1]

        try:
            file_content = base64.b64decode(file_b64)
        except (ValueError, UnicodeEncodeError):
            logging.warning("Le fichier n'est pas du Base64 pur. Traitement direct comme texte brut.")
            file_content = file_b64.encode('utf-8')

        # --- 1. Init Secrets & Token ---
        tenant_id = get_secret("tenantid")
        client_id = get_secret("clientid")
        client_secret = get_secret("appsecret")
        site_id = get_secret("siteid")
        
        import_list_id = get_secret("importlistid") 
        details_list_id = get_secret("cmddetailslistid") 
        config_list_id = get_secret("configlistid") 
        materiel_list_id=get_secret("materiellistid")
        materiel_reservation_list_id=get_secret("materielreservationlistid")
        

        token = get_graph_token(tenant_id, client_id, client_secret)
        if not token:
            return json_response({"status": "error", "message": "Échec de l'authentification Graph API."}, 500)

        # --- 2. Récupérer l'élément d'import ---
        import_item = graph_get_item_by_id(site_id, import_list_id, item_id, token)
        if not import_item:
            return json_response({"status": "error", "message": f"Élément d'import {item_id} introuvable dans SharePoint."}, 404)
        
        cmd_title = import_item.get("fields", {}).get("Title", "Inconnu")
        type_import = import_item.get("fields", {}).get("Type_import")
        aff_id = import_item.get("fields", {}).get("AFF_ID", "")

        if not type_import:
            return json_response({"status": "error", "message": "La colonne 'Type_import' est vide sur la commande.", "cmd_id": cmd_title}, 400)

        # --- 3. Purger les anciens détails ---
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

        statut_import = "oui"
        nouveaux_details = []
        nouveaux_materiels = [] # <-- NOUVEAU : Initialisation de la liste des matériels

        # --- 4. COMMUTATEUR (SWITCH) ---
        if type_import in ["Fichier prestation", "Fichier grossiste"]:
            try:
                df_total = get_excel_table_as_df(file_content, 'TabTotal')
                df_datas = get_excel_table_as_df(file_content, 'TabDatas')
            except ValueError as e:
                return json_response({"status": "error", "message": str(e), "cmd_id": cmd_title, "type_import": type_import}, 400)
            except Exception as e:
                return json_response({"status": "error", "message": "Fichier Excel invalide ou corrompu.", "details": str(e), "cmd_id": cmd_title, "type_import": type_import}, 400)
            
            total_prix_excel = str(df_total.iloc[0].get('TotalPrix', '')) if not df_total.empty else ""

            clef_config = 'TotalPrixTarifsPresta' if type_import == "Fichier prestation" else 'TotalPrixTarifsGrossiste'
            config_items = graph_filtered_items(site_id, config_list_id, token, f"fields/Clef eq '{clef_config}'")
            total_prix_bdd = config_items[0].get("fields", {}).get("Title", "") if config_items else ""

            if bypass != "ok" and total_prix_excel != total_prix_bdd:
                statut_import = "maj"
                # 1. On met à jour l'en-tête de la commande à "maj" tout de suite
                graph_update_field(site_id, import_list_id, item_id, token, {"StatutImport": statut_import})
                
                # 2. On STOPPE l'exécution et on renvoie un JSON spécifique à Power Automate
                logging.warning(f"Import bloqué pour {cmd_title} : Différence de prix détectée (Excel: {total_prix_excel} vs BDD: {total_prix_bdd})")
                
                return json_response({
                    "status": "maj", 
                    "cmd_id": cmd_title,
                    "type_import": type_import,
                    "lignes_creees": 0,
                    "statut_import_maj": statut_import,
                    "message": "Import suspendu : Différence de prix détectée. Veuillez mettre à jour les tarifs."
                }, 200)
            # --- NETTOYAGE ET PRÉPARATION ---
            # On remplace les "trous" (valeurs nulles/vides de l'Excel) par du texte vide pour éviter les bugs
            df_datas = df_datas.fillna("")
            
            for _, row in df_datas.iterrows():
                ligne_val = str(row.get('Ligne', '')).strip()
                ref_val = str(row.get('Référence', '')).strip()
                
                # --- NOUVELLE CONDITION (Comme FWSIM) ---
                # Si Ligne ET Référence sont vides, c'est une ligne vide de fin de tableau, on l'ignore.
                if ligne_val == "" and ref_val == "":
                    continue
                # ----------------------------------------
                
                # Calcul du Titre (Si ça commence par un espace, on prend la 2ème partie)
                titre = ligne_val.split(' ')[1].strip() if len(ligne_val.split(' ')) > 1 else ligne_val
                
                nouveaux_details.append({
                    "Title": titre,
                    "Reference": ref_val,
                    "Quantite": str(row.get('Quantité', '')).strip(),
                    "Statut": "Attente validation",
                    "CMD_ID": cmd_title
                })

        elif type_import == "Fichier Finale3D":
            try:
                csv_text = file_content.decode('utf-8') 
            except UnicodeDecodeError:
                csv_text = file_content.decode('latin-1')
            # Le names=range(10) force la création de 10 colonnes pour éviter le crash des lignes inégales
            df_csv = pd.read_csv(io.StringIO(csv_text), sep=';', header=None, dtype=str, names=range(10)).fillna("")
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
            try:
                csv_text = file_content.decode('utf-8') 
            except UnicodeDecodeError:
                csv_text = file_content.decode('latin-1')
            df_csv = pd.read_csv(io.StringIO(csv_text), sep=';', header=None, dtype=str, names=range(10)).fillna("")
            
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
            try:
                csv_text = file_content.decode('utf-8') 
            except UnicodeDecodeError:
                csv_text = file_content.decode('latin-1')
            df_csv = pd.read_csv(io.StringIO(csv_text), sep=';', header=None, dtype=str, names=range(15)).fillna("")
            
            chain_id = ""
            module_v = ""
            pin_v = ""

            for _, row in df_csv.iterrows():
                # 0 = Time | 1 = Réf
                col0 = str(row.get(0, "")).strip() 
                ref = str(row.get(1, "")).strip()
                
                # 1. On ignore SEULEMENT la ligne d'en-tête
                if "Time" in col0: 
                    continue
                    
                # 2. On ignore les lignes TOTALEMENT vides (fin de fichier)
                if col0 == "" and ref == "":
                    continue

                # 7 = Chain ID
                current_chain = str(row.get(7, "")).strip()
                if current_chain != chain_id and current_chain != "":
                    chain_id = current_chain
                    module_v = str(row.get(4, "")).strip()  # 4 = Module (E)
                    pin_v = str(row.get(5, "")).strip()     # 5 = Pin (F)

                qty = str(row.get(3, "")).strip()           # 3 = Quantité (D)
                col_address = str(row.get(9, "")).strip()   # 9 = Address (J)

                # Traitement du Titre (Variable "Ligne" dans Power Automate) 
                if col_address == chain_id:
                    titre = col_address
                elif col_address == "" or col_address.lower() == "nan": 
                    # Si pas d'adresse, on fait la combinaison Module-Pin
                    titre = f"{module_v}-{pin_v}"
                else:
                    # Sinon, on prend l'adresse
                    titre = col_address

                # --- NOUVEAU : On gère le matériel séparémentt ---
                if "MAT" in col_address.upper():  
                    nouveaux_materiels.append({
                        "Title": ref,
                        "Quantite": qty,
                        "Statut": "Attente validation",
                        "Aff_ID": str(aff_id),
                        "CMD_ID": str(cmd_title)
                    })
                else:
                    nouveaux_details.append({
                        "Title": titre,
                        "Reference": ref,
                        "Quantite": qty,
                        "Statut": "Attente validation",
                        "CMD_ID": cmd_title
                    })
        else:
            return json_response({"status": "error", "message": f"Type import inconnu: {type_import}", "cmd_id": cmd_title}, 400)


        # --- 4.5 VÉRIFICATION MATÉRIEL MANQUANT ---
        if nouveaux_materiels:
            logging.info("Vérification de l'existence des matériels dans la BDD...")
            unique_mat_refs = set([m["Title"] for m in nouveaux_materiels])
            missing_refs = []

            for mat_ref in unique_mat_refs:
                
                check_item = graph_filtered_items(site_id, materiel_list_id, token, f"fields/Title eq '{mat_ref}'")
                
                if not check_item:
                    missing_refs.append(mat_ref)

            if missing_refs:
                liste_manquants = ", ".join(missing_refs) # On prépare la liste propre (ex: "MAT1, MAT2")
                msg_erreur = f"Import bloqué : Les matériels suivants sont introuvables dans la base de données : {liste_manquants}"
                logging.error(msg_erreur)
                
                # On renvoie un statut 200 pour que PA continue, avec le statut spécifique
                return json_response({
                    "status": "materiel_manquant", 
                    "message": msg_erreur,
                    "materiels_manquants": liste_manquants, 
                    "cmd_id": cmd_title,
                    "type_import": type_import,
                    "lignes_creees": 0,
                    "lignes_materiel_creees": 0
                }, 200)

        # --- NOUVEAU : TRI DES LIGNES AVANT INSERTION ---
        def tri_intelligent(item):
            # Tente de convertir en nombre pour que "2" soit avant "10"
            val = str(item.get("Title", "")).strip()
            try:
                return (0, float(val))
            except ValueError:
                return (1, val)
                
        nouveaux_details.sort(key=tri_intelligent)
        # On trie aussi le matériel par ordre alphabétique de référence
        nouveaux_materiels.sort(key=lambda x: str(x.get("Title", "")))
        # ------------------------------------------------

        # --- 5. Insertion Massive ---
        logging.info(f"Création de {len(nouveaux_details)} nouvelles lignes de produits...")
        post_batch = []
        for index, item_payload in enumerate(nouveaux_details):
            #logging.info(f"-> [Produit] Ordre prévu : {index + 1} | Titre : {item_payload.get('Title')} | Réf : {item_payload.get('Reference')}")
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

        # --- NOUVEAU : Insertion Massive Matériel ---
        if nouveaux_materiels:
            logging.info(f"Création de {len(nouveaux_materiels)} nouvelles lignes de matériel...")
            post_batch_mat = []
            for index, item_payload in enumerate(nouveaux_materiels):
                #logging.info(f"-> [Matériel] Ordre prévu : {index + 1} | Titre : {item_payload.get('Title')}")
                post_batch_mat.append({
                    "id": str(index + 1),
                    "method": "POST",
                    "url": f"/sites/{site_id}/lists/{materiel_reservation_list_id}/items",
                    "body": {"fields": item_payload},
                    "headers": {"Content-Type": "application/json"}
                })
                if len(post_batch_mat) == 20:
                    graph_execute_batch(token, post_batch_mat)
                    post_batch_mat = []
            if post_batch_mat:
                graph_execute_batch(token, post_batch_mat)


        # --- 6. Mise à jour statut ---
        graph_update_field(site_id, import_list_id, item_id, token, {"StatutImport": statut_import})
        
        logging.info("Import terminé avec succès !")
        
        # --- RÉPONSE FINALE ENRICHIE ---
        return json_response({
            "status": "success", 
            "cmd_id": cmd_title,
            "type_import": type_import,
            "lignes_creees": len(nouveaux_details),
            "lignes_materiel_creees": len(nouveaux_materiels),
            "statut_import_maj": statut_import
        }, 200)

    except Exception as e:
        logging.exception("Erreur critique dans la fonction Azure Import")
        return json_response({
            "status": "error", 
            "message": "Erreur interne du serveur lors du traitement.", 
            "details": str(e),
            "cmd_id": cmd_title,
            "type_import": type_import
        }, 500)