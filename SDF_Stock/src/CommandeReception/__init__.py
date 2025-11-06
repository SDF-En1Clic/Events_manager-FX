import logging
import azure.functions as func
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
import requests
import urllib.parse
import json
from datetime import datetime

# --------- CONFIG -------------
VAULT_URL = "https://events-manager-kv.vault.azure.net/"  
# -----------------------------

def get_secret(name: str):
    credential = DefaultAzureCredential()
    client = SecretClient(vault_url=VAULT_URL, credential=credential)
    return client.get_secret(name).value

def graph_get_all(site_id, list_id, token, filter_expr=None):
    url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/items?$expand=fields"
    if filter_expr:
        url += f"&{filter_expr}"
    
    results = []
    while url:
        res = requests.get(url)
        res.raise_for_status()
        data = res.json()
        results.extend(data.get("value", []))
        url = data.get("@odata.nextLink")

    return results
def split_filter_queries(field_name, values, chunk_size=20):
    """
    Génère des filtres $filter par groupes (chunk_size) de valeurs.
    """
    filters = []
    for i in range(0, len(values), chunk_size):
        chunk = values[i:i + chunk_size]
        clause = " or ".join([f"{field_name} eq '{v}'" for v in chunk])
        filters.append(clause)
    return filters

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
        res = requests.get(url, headers=headers)
        res.raise_for_status()
        data = res.json()
        results.extend(data.get("value", []))
        url = data.get("@odata.nextLink")

    return results

def graph_update_field(site_id, list_id, item_id, token, updates: dict):
    url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/items/{item_id}/fields"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    res = requests.patch(url, headers=headers, data=json.dumps(updates))
    res.raise_for_status()



def graph_filtered_items(site_id, list_id, token, filter_expr=None):
    base_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/items?$expand=fields"
    headers = {
        "Authorization": f"Bearer {token}",
        "Prefer": "HonorNonIndexedQueriesWarningMayFailRandomly"
    }

    if filter_expr:
        # S'assurer que le filtre est encodé proprement 
        filter_param = urllib.parse.quote(filter_expr, safe="=()/ ")  # ne pas échapper les () ni eq, ni espaces
        base_url += f"&$filter={filter_param}"

    results = []
    url = base_url

    # --- AJOUT DE LOG (MODIFIÉ) ---
    logging.info(f"Appel Graph API (filtré): {url}")
    # --- FIN AJOUT ---

    while url:
        res = requests.get(url, headers=headers)
        
        # --- AJOUT DE LOG D'ERREUR ---
        if not res.ok: 
             logging.error(f"Erreur API Graph (filtré). Status: {res.status_code}. Réponse: {res.text}")
        # --- FIN AJOUT ---

        res.raise_for_status()
        data = res.json()
        results.extend(data.get("value", []))
        url = data.get("@odata.nextLink")

    return results

# --- NOUVELLE FONCTION AJOUTÉE ---
def graph_get_item_by_id(site_id, list_id, item_id, token):
    """
    Récupère un seul élément de liste par son ID SharePoint natif.
    C'est plus efficace qu'un filtre.
    """
    base_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/items/{item_id}?$expand=fields"
    headers = {
        "Authorization": f"Bearer {token}"
    }

    logging.info(f"Appel Graph API (Get Item by ID): {base_url}")

    try:
        res = requests.get(base_url, headers=headers)
        
        if not res.ok:
            logging.error(f"Erreur API Graph (Get Item by ID). Status: {res.status_code}. Réponse: {res.text}")
        
        res.raise_for_status()
        
        return res.json()  # Renvoie l'objet item complet

    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 404:
            logging.warning(f"Élément introuvable (404) à l'URL : {base_url}")
        else:
            logging.error(f"Erreur HTTP inattendue (Get Item by ID): {e}")
        return None # Renvoie None en cas d'erreur HTTP (ex: 404 Not Found)
    except Exception as e:
        logging.error(f"Erreur non-HTTP (Get Item by ID): {e}")
        return None
# --- FIN DE LA NOUVELLE FONCTION ---

# --- NOUVELLE FONCTION AJOUTÉE (version avec logging) ---
def get_site_name(site_id, token):
    """
    Récupère le nom d'affichage d'un site SharePoint à partir de son ID via l'API Graph.
    Utilise logging pour les erreurs.
    """
    url = f"https://graph.microsoft.com/v1.0/sites/{site_id}"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    try:
        res = requests.get(url, headers=headers)
        res.raise_for_status()  # Lève une exception en cas d'erreur HTTP (4xx ou 5xx)
        
        data = res.json()
        
        # Le nom du site est généralement dans 'displayName' ou 'name'
        site_name = data.get('displayName') or data.get('name')
        
        if not site_name:
            logging.warning(f"Avertissement: L'ID du site {site_id} est valide, mais n'a pas retourné de nom.")
            
        return site_name

    except requests.exceptions.HTTPError as http_err:
        logging.error(f"Erreur HTTP lors de la récupération du nom du site: {http_err}")
        logging.error(f"Réponse: {res.text}")
        if res.status_code == 404:
            logging.error(f"Erreur: Le site avec l'ID '{site_id}' n'a pas été trouvé.")
        elif res.status_code == 401:
            logging.error("Erreur: Le token est invalide ou a expiré.")
    except Exception as err:
        logging.error(f"Une autre erreur est survenue lors de la récupération du nom du site: {err}")
        
    return None
# --- FIN DE LA FONCTION AJOUTÉE ---

# --- NOUVELLE FONCTION POUR VÉRIFIER LE NOM DE LA LISTE ---
def get_list_name(site_id, list_id, token):
    """
    Récupère le nom d'affichage d'une liste SharePoint à partir de son ID.
    """
    url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    try:
        res = requests.get(url, headers=headers)
        res.raise_for_status()
        data = res.json()
        list_name = data.get('displayName') or data.get('name')
        return list_name
    except requests.exceptions.HTTPError as http_err:
        logging.error(f"Erreur HTTP lors de la récupération du nom de la liste {list_id}: {http_err}")
    except Exception as err:
        logging.error(f"Erreur lors de la récupération du nom de la liste {list_id}: {err}")
    return None
# --- FIN DE LA FONCTION AJOUTÉE ---


def get_graph_token(tenant_id, client_id, client_secret):
    """
    Obtient un token d'accès 'client_credentials' pour Microsoft Graph.
    
    Cette fonction inclut une gestion d'erreurs robuste pour s'assurer
    qu'un token valide est retourné.
    """
    url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
    data = {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret,
        "scope": "https://graph.microsoft.com/.default"
    }
    
    headers = {
        "Content-Type": "application/x-www-form-urlencoded"
    }

    try:
        # Envoyer la requête POST pour obtenir le token
        response = requests.post(url, data=data, headers=headers)
        
        # --- AJOUT DE LA GESTION D'ERREURS ---
        
        # Vérifier si la requête a échoué (status code 4xx ou 5xx)
        if not response.ok:
            logging.error(f"Échec de l'obtention du token. Status: {response.status_code}.")
            # Logguer la réponse d'erreur de Microsoft pour le débogage
            logging.error(f"Réponse d'erreur (Token): {response.text}")
            # Lever une exception pour être attrapée par le bloc 'except'
            response.raise_for_status() 

        # Si la requête réussit (status 200 OK)
        response_json = response.json()
        access_token = response_json.get("access_token")

        if not access_token:
            # Cas rare où la réponse est 200 OK mais sans token
            logging.error(f"Réponse OK (200) pour le token, mais 'access_token' est manquant. Réponse: {response_json}")
            return None
        
        logging.info("Token d'accès Microsoft Graph obtenu avec succès.")
        return access_token

    except requests.exceptions.HTTPError as e:
        # L'erreur a déjà été loggée ci-dessus
        logging.error(f"Erreur HTTP lors de la demande de token: {e}")
        return None
    except requests.exceptions.RequestException as e:
        # Pour les autres erreurs (ex: problème de connexion, DNS)
        logging.error(f"Erreur de requête (connexion?) lors de la demande de token: {e}")
        return None
    except Exception as e:
        # Pour les erreurs inattendues (ex: échec de .json() si la réponse n'est pas JSON)
        logging.error(f"Erreur inattendue lors de la demande de token: {e}")
        return None

def parse_float(value):
    try:
        return float(value)
    except:
        return 0


def main(req: func.HttpRequest) -> func.HttpResponse:
    try:
        body = req.get_json()
        commande_id = body.get("commande_id")
        site_recept= body.get("site")
        batiment_recept = body.get("batiment")
        emplacement_recept = body.get("emplacement")
                # --- LOG AJOUTÉ ---
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
        logging.info("Récupération du token Graph...")
        token = get_graph_token(tenant_id, client_id, client_secret)
        if not token:
            logging.error("Échec de l'obtention du token Graph.")
            return func.HttpResponse("Échec de l'authentification Graph", status_code=500)
        logging.info("Token Graph obtenu.")

        # --- VÉRIFICATION DU NOM DU SITE (AJOUTÉ) ---
        try:
            nom_site = get_site_name(site_id, token)
            if nom_site:
                logging.info(f"Connecté au site SharePoint: '{nom_site}' (ID: {site_id})")
            else:
                logging.warning(f"Impossible de vérifier le nom du site pour l'ID: {site_id}")
        except Exception as e:
            logging.warning(f"Erreur lors de la vérification du nom du site: {e}")
        # --- FIN DE LA VÉRIFICATION ---

        # --- VÉRIFICATION DU NOM DE LA LISTE (AJOUTÉ) ---
        try:
            nom_liste = get_list_name(site_id, commandes_list_id, token)
            if nom_liste:
                logging.info(f"Tentative de récupération de la commande depuis la liste: '{nom_liste}' (ID: {commandes_list_id})")
            else:
                logging.warning(f"Impossible de vérifier le nom de la liste pour l'ID: {commandes_list_id}")
        except Exception as e:
            logging.warning(f"Erreur lors de la vérification du nom de la liste: {e}")
        # --- FIN VÉRIFICATION LISTE ---

        logging.info(f"Récupération de la commande ID: {commande_id}")

        # --- MODIFICATION: Utilisation de la nouvelle fonction Get Item by ID ---
        try:
            commande_item = graph_get_item_by_id(site_id, commandes_list_id, commande_id, token)
            
            if not commande_item:
                logging.warning(f"Commande {commande_id} introuvable (ou erreur 404).")
                return func.HttpResponse("Commande introuvable", status_code=404)
            
            # La fonction renvoie directement l'item, pas une liste
            commande = commande_item.get("fields") 
            if not commande:
                 logging.error(f"Commande {commande_id} trouvée mais le champ 'fields' est manquant.")
                 return func.HttpResponse("Erreur de format de commande", status_code=500)

            logging.info(f"Commande {commande_id} trouvée.")

        except requests.exceptions.HTTPError as e:
            # Ce bloc ne devrait plus être atteint si graph_get_item_by_id gère les 404
            logging.error(f"Erreur HTTP lors de la récupération de la commande: {e}")
            return func.HttpResponse("Commande introuvable (erreur HTTP)", status_code=404)
        # --- FIN DE LA MODIFICATION ---

        site_stock = commande.get("Site_Stock")
        site_stock_bis = commande.get("Site_Stock_second")
        date_livraison = commande.get("Date_livraison")
        if date_livraison:
            try:
                date_livraison = datetime.strptime(date_livraison[:10], "%Y-%m-%d")
            except:
                date_livraison = None
       
        details = graph_filtered_items(site_id, details_list_id, token, f"fields/CMD_ID eq {commande_id}")



        produits = graph_list_items(site_id, produits_list_id, token)
        inventaire = graph_list_items(site_id, inventaire_list_id, token)
        arrivages = graph_list_items(site_id, arrivages_list_id, token)

        # Extraire toutes les références (Title) distinctes utilisées dans les détails de la commande
        references_utiles = list(set(d["fields"].get("Title") for d in details if "fields" in d and d["fields"].get("Title")))

        # Construire le filtre Graph API (limité en taille !)
        # reference_filters = " or ".join([f"fields/Title eq '{ref}'" for ref in references_utiles])
        filter_clauses = split_filter_queries("fields/Title", references_utiles, chunk_size=20)


        # 2. Fait plusieurs appels à Graph API, un pour chaque bloc
        all_details = []
        for clause in filter_clauses:
            all_details.extend(
                graph_filtered_items(site_id, details_list_id, token, filter_expr=clause)
            )

        # Optionnel : compter le nombre de lignes pour vérification
        logging.info(f"Nombre de lignes de détails récupérées : {len(details)}")
        logging.info(f"Nombre de lignes de détails total : {len(all_details)}")
        nb_lignes_commande = len(details)
        ruptures = []
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
            logging.info(" Vérification du produit : %s", reference)
            logging.info("    Quantité demandée : %s", quantite)
            logging.info("    Statut du détail : %s", statut)
            logging.info("    Origine du produit : %s", origine)
            
            if origine == "SDF":
                if statut == "Rupture (SdF)":
                    # Vérifie site principal
                    q_inv = sum(
                        parse_float(i["fields"].get("Quantite"))
                        for i in inventaire
                        if i["fields"].get("Title") == reference and i["fields"].get("Site") == site_stock
                    )
                    q_resa = sum(
                        parse_float(l["fields"].get("Quantite"))
                        for l in all_details
                        if l["fields"].get("Reference") == reference
                        and l["fields"].get("Statut") in ["Reservé", "Préparé", "Sortie produits"]
                        and (l["fields"].get("Site") == site_stock)
                        and (l["fields"].get("Statut") != "Sortie produits" or l["fields"].get("Comptabilise_inventaire") != 1)
                    )
                    
                    logging.info("   Produit éligible au contrôle de stock (origine SDF)")
                    logging.info("   Site principal : %s", site_stock)
                    logging.info("   q_inv (stock) site principal : %s", q_inv)
                    logging.info("   q_resa (réservé) site principal : %s", q_resa)
                    logging.info("   dispo = q_inv - q_resa : %s", dispo)
                    
                    dispo = q_inv - q_resa
                    if dispo >= quantite:
                        # graph_update_field(site_id, details_list_id, item_id, token, {"Statut_prepa": "Préparé","Statut": "Préparé","Site_prepa":site_recept, "Batiment_prepa":batiment_recept, "Emplacement_prepa":emplacement_recept})
                        continue  # Produit validé dans site principal
                    
                    # Vérifie site secondaire
                    if site_stock_bis and site_stock_bis != "0":
                        q_inv_bis = sum(
                            parse_float(i["fields"].get("Quantite"))
                            for i in inventaire
                            if i["fields"].get("Title") == reference and i["fields"].get("Site") == site_stock_bis
                        )
                        q_resa_bis = sum(
                            parse_float(l["fields"].get("Quantite"))
                            for l in all_details
                            if l["fields"].get("Reference") == reference
                            and l["fields"].get("Statut") in ["Reservé", "Préparé", "Sortie produits"]
                            and (l["fields"].get("Site") == site_stock_bis)
                            and (l["fields"].get("Statut") != "Sortie produits" or l["fields"].get("Comptabilise_inventaire") != 1)
                        )
                        
                        logging.info("   ➤ Site secondaire : %s", site_stock_bis)
                        logging.info("   ➤ q_inv_bis (stock) : %s", q_inv_bis)
                        logging.info("   ➤ q_resa_bis (réservé) : %s", q_resa_bis)
                        logging.info("   ➤ dispo_bis = q_inv_bis - q_resa_bis : %s", dispo_bis)
                        
                        dispo_bis = q_inv_bis - q_resa_bis
                        if dispo_bis >= quantite:
                            # graph_update_field(site_id, details_list_id, item_id, token, {"Statut_prepa": "Préparé","Statut": "Préparé","Site_prepa":site_recept, "Batiment_prepa":batiment_recept, "Emplacement_prepa":emplacement_recept})
                            continue  # Produit validé dans site secondaire

                    ruptures.append({"reference": reference, "raison": "stock et arrivage insuffisants"})
                else:
                    # graph_update_field(site_id, details_list_id, item_id, token, {"Statut_prepa": "Préparé","Statut": "Préparé","Site_prepa":site_recept, "Batiment_prepa":batiment_recept, "Emplacement_prepa":emplacement_recept})

            else:
                logging.info("   ➤ Produit non SDF – pas de contrôle de stock (considéré disponible)")
                # graph_update_field(site_id, details_list_id, item_id, token, {"Statut_prepa": "Préparé","Statut": "Préparé","Site":site_recept, "Batiment":batiment_recept, "Emplacement":emplacement_recept,"Site_prepa":site_recept, "Batiment_prepa":batiment_recept, "Emplacement_prepa":emplacement_recept})

        # graph_update_field(site_id, commandes_list_id, commande_id, token, {"Statut": "Réceptionné"})
        statut_final = "Validé" if not ruptures else "Validé (Rupture SdF)"
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


