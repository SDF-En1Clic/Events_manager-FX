import logging
import azure.functions as func
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
import requests
import urllib.parse
import json
from datetime import datetime

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
    headers = { "Content-Type": "application/x-www-form-urlencoded" }

    try:
        response = session.post(url, data=data, headers=headers)
        if not response.ok:
            logging.error(f"Échec Token. Status: {response.status_code}. Resp: {response.text}")
            response.raise_for_status()

        access_token = response.json().get("access_token")
        if not access_token:
            return None
        return access_token

    except Exception as e:
        logging.error(f"Erreur token: {e}")
        return None

def graph_list_items(site_id, list_id, token, filter_expr=None):
    headers = {
        "Authorization": f"Bearer {token}",
        "Prefer": "HonorNonIndexedQueriesWarningMayFailRandomly"
    }
    url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/items?$expand=fields"
    if filter_expr:
        url += f"&{filter_expr}"

    results = []
    while url:
        res = session.get(url, headers=headers)
        res.raise_for_status()
        data = res.json()
        results.extend(data.get("value", []))
        url = data.get("@odata.nextLink")
    return results

def graph_filtered_items(site_id, list_id, token, filter_expr=None):
    base_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/items?$expand=fields"
    headers = {
        "Authorization": f"Bearer {token}",
        "Prefer": "HonorNonIndexedQueriesWarningMayFailRandomly"
    }

    if filter_expr:
        filter_param = urllib.parse.quote(filter_expr, safe="=()/ ")
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

    return results

def graph_get_item_by_id(site_id, list_id, item_id, token):
    url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/items/{item_id}?$expand=fields"
    headers = { "Authorization": f"Bearer {token}" }

    try:
        res = session.get(url, headers=headers)
        if not res.ok:
            logging.error(f"Erreur Get Item {item_id}. Status: {res.status_code}")
        res.raise_for_status()
        return res.json()
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 404:
            logging.warning(f"Item {item_id} introuvable (404).")
        return None
    except Exception as e:
        logging.error(f"Erreur Get Item: {e}")
        return None

def graph_update_field(site_id, list_id, item_id, token, updates: dict):
    url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/items/{item_id}/fields"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    res = session.patch(url, headers=headers, data=json.dumps(updates))
    
    if not res.ok:
        logging.error(f"Erreur Update Item {item_id}. Status: {res.status_code}. Resp: {res.text}")
    
    res.raise_for_status()

def parse_float(value):
    try:
        return float(value)
    except:
        return 0.0

# ==============================================================================
# FONCTION PRINCIPALE : CommandeVerificationMateriel
# ==============================================================================
def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Début du traitement Vérification Matériel.')

    try:
        body = req.get_json()
        commande_id = body.get("commande_id")
        logging.info(f"Paramètre 'commande_id' reçu : {commande_id}")
        
        if not commande_id:
            return func.HttpResponse(json.dumps({"error": "Paramètre 'commande_id' requis"}), status_code=400, mimetype="application/json")

        # --- Secrets ---
        tenant_id = get_secret("tenantid")
        client_id = get_secret("clientid")
        client_secret = get_secret("appsecret")
        site_id = get_secret("siteid")
        
        cmd_list_id = get_secret("cmdlistid")
        materiel_stock_list_id = get_secret("materielstocklistid") 
        materiel_reservation_list_id = get_secret("materielreservationlistid")
        affaireevtslistid = get_secret("affaireevtslistid")

        # --- Auth ---
        token = get_graph_token(tenant_id, client_id, client_secret)
        if not token:
            return func.HttpResponse(json.dumps({"error": "Échec de l'authentification Graph API"}), status_code=500, mimetype="application/json")

        # --- 1. Récupération de la commande pour avoir la date de l'événement ---
        commande_item = graph_get_item_by_id(site_id, cmd_list_id, commande_id, token)
        if not commande_item:
            return func.HttpResponse(json.dumps({"error": f"Commande {commande_id} introuvable"}), status_code=404, mimetype="application/json")
        
        commande = commande_item.get("fields", {})
        
        aff_id = commande.get("Aff_ID") or commande.get("AFF_ID")
        date_evenement = None
        
        if aff_id:
            logging.info(f"Aff_ID trouvé : {aff_id}. Recherche de l'événement (ID={aff_id}) dans affaireevtslistid...")
            evt_item = graph_get_item_by_id(site_id, affaireevtslistid, aff_id, token)
            if evt_item:
                evt_fields = evt_item.get("fields", {})
                date_evt_brute = evt_fields.get("Date_evt")
                if date_evt_brute:
                    date_evenement = date_evt_brute[:10]
            else:
                logging.warning(f"Aucun événement trouvé pour l'ID {aff_id}")
        else:
            logging.warning("Aucun Aff_ID trouvé sur la commande.")
        
        logging.info(f"Date de l'événement ciblée : {date_evenement}")

        # --- 2. Récupération du stock global des matériels ---
        logging.info("Récupération de materielstocklistid...")
        stock_items = graph_list_items(site_id, materiel_stock_list_id, token)
        
        # Création d'un dictionnaire { "Reference": Quantite_Total_Stock }
        stock_total_map = {}
        for item in stock_items:
            f = item.get("fields", {})
            ref = f.get("Title") # ⚠️ On suppose que la référence est dans Title
            qty = parse_float(f.get("Quantite"))
            if ref:
                stock_total_map[ref] = qty

        # --- 3. Récupération des réservations validées ---
        logging.info("Récupération des matériels réservés (Statut = Validé)...")
        reservations_validees = graph_filtered_items(site_id, materiel_reservation_list_id, token, "fields/Statut eq 'Validé'")
        
        # Calcul de la somme réservée par référence POUR CETTE DATE
        somme_reservee_map = {}
        for res in reservations_validees:
            f = res.get("fields", {})
            ref = f.get("Title") # ⚠️ La référence sur la ligne de réservation
            # On vérifie si la ligne de réservation tombe sur la même date 
            date_res = f.get("Date_reservation")
            
            if date_evenement and date_res and date_res[:10] == date_evenement:
                qty = parse_float(f.get("Quantite"))
                if ref:
                    somme_reservee_map[ref] = somme_reservee_map.get(ref, 0.0) + qty

        # --- 4. Calcul du disponible par référence ---
        dispo_map = {}
        for ref, qte_total in stock_total_map.items():
            qte_reservee = somme_reservee_map.get(ref, 0.0)
            dispo_map[ref] = qte_total - qte_reservee

        # --- 5. Mise à jour des lignes de la commande cible ---
        logging.info(f"Récupération des lignes de matériel pour la commande {commande_id}...")
        
        # ⚠️ J'utilise "CMD_ID" comme demandé. Si tu stockes l'ID dans "AFF_ID" pour les matériels, change CMD_ID par AFF_ID ici :
        lignes_commande = graph_filtered_items(site_id, materiel_reservation_list_id, token, f"fields/CMD_ID eq '{commande_id}'")
        
        lignes_mises_a_jour = 0
        for ligne in lignes_commande:
            item_id = ligne["id"]
            f = ligne.get("fields", {})
            ref = f.get("Title")
            
            if ref and ref in dispo_map:
                qte_dispo_calculee = dispo_map[ref]
                
                # Mise à jour de la colonne qte_dispo dans SharePoint
                graph_update_field(site_id, materiel_reservation_list_id, item_id, token, {
                    "qte_dispo": qte_dispo_calculee
                })
                lignes_mises_a_jour += 1

        logging.info(f"Traitement terminé. {lignes_mises_a_jour} lignes mises à jour.")

        retour = {
            "commande_id": commande_id,
            "statut": "success",
            "lignes_mises_a_jour": lignes_mises_a_jour
        }
        return func.HttpResponse(json.dumps(retour), status_code=200, mimetype="application/json")

    except Exception as e:
        logging.exception("Erreur critique dans la fonction Azure CommandeVerificationMateriel")
        return func.HttpResponse(json.dumps({"error": f"Erreur serveur : {str(e)}"}), status_code=500, mimetype="application/json")