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

# --- OPTIMISATION : SESSION PERSISTANTE ---
session = requests.Session()
adapter = requests.adapters.HTTPAdapter(pool_connections=100, pool_maxsize=100)
session.mount('https://', adapter)
# ------------------------------------------

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
        return response.json().get("access_token")
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
    # Utilisation de session.patch pour éviter l'erreur DNS
    res = session.patch(url, headers=headers, data=json.dumps(updates))
    if not res.ok:
        logging.error(f"Erreur Update Item {item_id}. Status: {res.status_code}. Resp: {res.text}")
    res.raise_for_status()

def get_site_name(site_id, token):
    url = f"https://graph.microsoft.com/v1.0/sites/{site_id}"
    headers = { "Authorization": f"Bearer {token}" }
    try:
        res = session.get(url, headers=headers)
        res.raise_for_status()
        data = res.json()
        return data.get('displayName') or data.get('name')
    except Exception:
        return None

def get_list_name(site_id, list_id, token):
    url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}"
    headers = { "Authorization": f"Bearer {token}" }
    try:
        res = session.get(url, headers=headers)
        res.raise_for_status()
        data = res.json()
        return data.get('displayName') or data.get('name')
    except Exception:
        return None

def split_filter_queries(field_name, values, chunk_size=20):
    filters = []
    for i in range(0, len(values), chunk_size):
        chunk = values[i:i + chunk_size]
        clause = " or ".join([f"{field_name} eq '{v}'" for v in chunk])
        filters.append(clause)
    return filters

def parse_float(value):
    try:
        return float(value)
    except:
        return 0.0

# ==============================================================================
# MAIN
# ==============================================================================
def main(req: func.HttpRequest) -> func.HttpResponse:
    try:
        body = req.get_json()
        commande_id = body.get("commande_id")
        logging.info(f"Paramètre 'commande_id' reçu : {commande_id}")
        if not commande_id:
            return func.HttpResponse("Paramètre 'commande_id' requis", status_code=400)

        # --- Secrets
        tenant_id = get_secret("tenantid")
        client_id = get_secret("clientid")
        client_secret = get_secret("appsecret")
        site_id = get_secret("siteid")
        commandes_list_id = get_secret("cmdlistid")
        details_list_id = get_secret("cmddetailslistid")
        produits_list_id = get_secret("produitslistid")
        inventaire_list_id = get_secret("inventairelistid")
        arrivages_list_id = get_secret("arrivagesproduitslistid")

        # --- Auth
        token = get_graph_token(tenant_id, client_id, client_secret)
        if not token:
            return func.HttpResponse("Échec de l'authentification Graph", status_code=500)

        # --- Logs de vérification
        nom_site = get_site_name(site_id, token)
        logging.info(f"Site : {nom_site}")

        # --- Commande
        commande_item = graph_get_item_by_id(site_id, commandes_list_id, commande_id, token)
        if not commande_item:
            return func.HttpResponse("Commande introuvable", status_code=404)
        
        commande = commande_item.get("fields", {})
        
        site_stock = commande.get("Site_Stock")
        site_stock_bis = commande.get("Site_Stock_second")
        date_livraison = commande.get("Date_livraison")
        if date_livraison:
            try:
                date_livraison = datetime.strptime(date_livraison[:10], "%Y-%m-%d")
            except:
                date_livraison = None

        # --- Récupération des détails (lignes de commande)
        logging.info("Récupération des détails...")
        details = graph_filtered_items(site_id, details_list_id, token, f"fields/CMD_ID eq {commande_id}")
        nb_lignes_commande = len(details)
        logging.info(f"Nombre de lignes : {nb_lignes_commande}")

        # --- Chargement global
        logging.info("Chargement global (Produits, Inventaire, Arrivages)...")
        produits = graph_list_items(site_id, produits_list_id, token)
        inventaire = graph_list_items(site_id, inventaire_list_id, token)
        arrivages = graph_list_items(site_id, arrivages_list_id, token)

        # --- Historique (Batch)
        references_utiles = list(set(d["fields"].get("Title") for d in details if "fields" in d and d["fields"].get("Title")))
        filter_clauses = split_filter_queries("fields/Title", references_utiles, chunk_size=20)
        
        all_details_history = []
        logging.info("Chargement historique réservations...")
        for clause in filter_clauses:
            all_details_history.extend(
                graph_filtered_items(site_id, details_list_id, token, filter_expr=clause)
            )
        logging.info(f"Historique : {len(all_details_history)} lignes.")

        # --- TRACKER DE STOCK (Pour gérer les doublons)
        usage_tracker = {}
        ruptures = []

        # --- BOUCLE
        for detail in details:
            d = detail["fields"]
            reference = d.get("Reference")
            item_id = detail["id"] 
            quantite = parse_float(d.get("Quantite"))
            statut = d.get("Statut")

            produit = next((p["fields"] for p in produits if p["fields"].get("Title") == reference), None)
            if not produit:
                ruptures.append({"reference": reference, "raison": "produit introuvable"})
                continue

            origine = produit.get("Origine", "")
            logging.info(f"Check: {reference} | Origine: {origine} | Qté: {quantite}")

            # ------------------------------------------------
            # LOGIQUE SDF
            # ------------------------------------------------
            if origine == "SDF":

                # 1. Site Principal
                q_inv = sum(
                    parse_float(i["fields"].get("Quantite")) for i in inventaire
                    if i["fields"].get("Title") == reference and i["fields"].get("Site") == site_stock
                )
                
                batiment = None
                emplacement = None
                for i in inventaire:
                    f = i.get("fields", {})
                    if f.get("Title") == reference and f.get("Site") == site_stock:
                        batiment = f.get("Batiment")
                        emplacement = f.get("Emplacement")
                        break
                
                q_resa = sum(
                    parse_float(l["fields"].get("Quantite")) for l in all_details_history
                    if l["fields"].get("Reference") == reference
                    and l["fields"].get("Statut") in ["Reservé", "Préparé", "Sortie produits"]
                    and (l["fields"].get("Site") == site_stock)
                    and (l["fields"].get("Statut") != "Sortie produits" or l["fields"].get("Comptabilise_inventaire") != 1)
                )

                # Tracker Local
                key_main = f"{reference}_main_{site_stock}"
                deja_pris = usage_tracker.get(key_main, 0.0)
                dispo = q_inv - q_resa - deja_pris

                if dispo >= quantite:
                    graph_update_field(site_id, details_list_id, item_id, token, 
                        {"Statut": "Reservé", "Site":site_stock, "Batiment":batiment, "Emplacement":emplacement})
                    usage_tracker[key_main] = deja_pris + quantite
                    continue 

                # 2. Site Secondaire
                if site_stock_bis and site_stock_bis != "0":
                    q_inv_bis = sum(
                        parse_float(i["fields"].get("Quantite")) for i in inventaire
                        if i["fields"].get("Title") == reference and i["fields"].get("Site") == site_stock_bis
                    )
                    batiment_bis = None
                    emplacement_bis = None
                    for i in inventaire:
                        f = i.get("fields", {})
                        if f.get("Title") == reference and f.get("Site") == site_stock_bis:
                            batiment_bis = f.get("Batiment")
                            emplacement_bis = f.get("Emplacement")
                            break
                    
                    q_resa_bis = sum(
                        parse_float(l["fields"].get("Quantite")) for l in all_details_history
                        if l["fields"].get("Reference") == reference
                        and l["fields"].get("Statut") in ["Reservé", "Préparé", "Sortie produits"]
                        and (l["fields"].get("Site") == site_stock_bis)
                        and (l["fields"].get("Statut") != "Sortie produits" or l["fields"].get("Comptabilise_inventaire") != 1)
                    )
                    
                    key_sec = f"{reference}_sec_{site_stock_bis}"
                    deja_pris_sec = usage_tracker.get(key_sec, 0.0)
                    dispo_bis = q_inv_bis - q_resa_bis - deja_pris_sec

                    if dispo_bis >= quantite:
                        graph_update_field(site_id, details_list_id, item_id, token, 
                            {"Statut": "Reservé", "Site":site_stock_bis, "Batiment":batiment_bis, "Emplacement":emplacement_bis})
                        usage_tracker[key_sec] = deja_pris_sec + quantite
                        continue 

                # 3. Arrivage
                q_arriv = sum(
                    parse_float(a["fields"].get("Quantite")) for a in arrivages
                    if a["fields"].get("Title") == reference and date_livraison and a["fields"].get("Date")
                    and datetime.strptime(a["fields"]["Date"][:10], "%Y-%m-%d") < date_livraison
                )
                q_en_cours = sum(
                    parse_float(l["fields"].get("Quantite")) for l in all_details_history
                    if l["fields"].get("Reference") == reference and l["fields"].get("Statut") == "Arrivage"
                )
                
                key_arriv = f"{reference}_arrivage"
                deja_pris_arriv = usage_tracker.get(key_arriv, 0.0)
                dispo_arriv = (q_arriv - q_en_cours) - deja_pris_arriv

                if dispo_arriv >= quantite:
                    graph_update_field(site_id, details_list_id, item_id, token, {"Statut": "Arrivage"})
                    usage_tracker[key_arriv] = deja_pris_arriv + quantite
                    continue 
                
                # Rupture (Note: le code d'origine commentait l'update du champ, je le laisse commenté mais je note la rupture)
                # graph_update_field(site_id, details_list_id, item_id, token, {"Statut": "Rupture SdF"})
                ruptures.append({"reference": reference, "raison": "stock et arrivage insuffisants"})

            # ------------------------------------------------
            # LOGIQUE NON-SDF
            # ------------------------------------------------
            else:
                logging.info("   ➤ Produit non SDF – Passage en 'Commandé'")
                graph_update_field(site_id, details_list_id, item_id, token, {"Statut": "Commandé"})
                continue

        # --- MISE A JOUR DU STATUT DE LA COMMANDE
        if not ruptures: 
            statut_final = "OK" 
            graph_update_field(site_id, commandes_list_id, commande_id, token, {"Statut": "Validé"})
        else:
            statut_final = "Rupture" 
            graph_update_field(site_id, commandes_list_id, commande_id, token, {"Statut": "Validé (Rupture SdF)"})
            
        retour = {
            "commande_id": commande_id,
            "statut": statut_final,
            "ruptures": ruptures,
            "nb_produits_commande": nb_lignes_commande
        }

        return func.HttpResponse(
            json.dumps(retour),
            status_code=200,
            mimetype="application/json"
        )

    except Exception as e:
        logging.exception("Erreur dans la fonction Azure")
        return func.HttpResponse(f"Erreur serveur : {str(e)}", status_code=500)
