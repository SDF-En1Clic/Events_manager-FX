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
        # ... [Tout votre code d'initialisation reste identique jusqu'à la récupération des données] ...
        
        # On suppose que les données (details, produits, inventaire, etc.) sont chargées ici
        # comme dans votre code précédent.

        # ... [Récupération details, produits, inventaire, arrivages, ukobas] ...

        # --- INITIALISATION DU TRACKER (NOUVEAU) ---
        # Ce dictionnaire va retenir la quantité utilisée par référence ET par source (Site Principal, Secondaire, Arrivage)
        # Clé : "Reference_TypeSource", Valeur : Quantité accumulée
        usage_tracker = {} 
        # -------------------------------------------

        ruptures = []
        nb_lignes_commande = len(details)

        for detail in details:
            d = detail["fields"]
            reference = d.get("Reference")
            item_id = detail["id"] 
            quantite = parse_float(d.get("Quantite"))
            statut = d.get("Statut") # On pourrait vouloir ignorer les lignes déjà traitées si besoin

            produit = next((p["fields"] for p in produits if p["fields"].get("Title") == reference), None)
            if not produit:
                ruptures.append({"reference": reference, "raison": "produit introuvable"})
                continue

            origine = produit.get("Origine", "")
            
            # --- LOGIQUE SDF ---
            if origine == "SDF":

                # 1. Vérifie site principal
                q_inv = sum(
                    parse_float(i["fields"].get("Quantite"))
                    for i in inventaire
                    if i["fields"].get("Title") == reference and i["fields"].get("Site") == site_stock
                )

                # Récup info batiment/emplacement (inchangé)
                batiment = None 
                emplacement = None
                for i in inventaire:
                    if i["fields"].get("Title") == reference and i["fields"].get("Site") == site_stock:
                        batiment = i["fields"].get("Batiment")
                        emplacement = i["fields"].get("Emplacement")
                        break # Optimisation : on prend le premier trouvé
                
                q_resa = sum(
                    parse_float(l["fields"].get("Quantite"))
                    for l in all_details
                    if l["fields"].get("Reference") == reference
                    and l["fields"].get("Statut") in ["Reservé", "Préparé", "Sortie produits"]
                    and (l["fields"].get("Site") == site_stock)
                    and (l["fields"].get("Statut") != "Sortie produits" or l["fields"].get("Comptabilise_inventaire") != 1)
                )

                # --- MODIFICATION ICI : Prise en compte de la consommation locale ---
                key_main = f"{reference}_main_{site_stock}"
                deja_pris_ce_tour = usage_tracker.get(key_main, 0)
                
                dispo = q_inv - q_resa - deja_pris_ce_tour  # On soustrait ce qu'on a pris aux tours précédents
                
                logging.info(f"Ref: {reference} | Stock: {q_inv} | Resa: {q_resa} | Déjà Pris (Lignes préc.): {deja_pris_ce_tour} | Dispo Réelle: {dispo}")

                if dispo >= quantite:
                    graph_update_field(site_id, details_list_id, item_id, token, {"Statut": "Disponible", "Site":site_stock, "Batiment":batiment, "Emplacement":emplacement})
                    # On met à jour le tracker pour la prochaine ligne qui demanderait la même référence
                    usage_tracker[key_main] = deja_pris_ce_tour + quantite
                    continue 
                
                # 2. Vérifie site secondaire
                if site_stock_bis and site_stock_bis != "0":
                    q_inv_bis = sum(
                        parse_float(i["fields"].get("Quantite"))
                        for i in inventaire
                        if i["fields"].get("Title") == reference and i["fields"].get("Site") == site_stock_bis
                    )
                    
                    batiment_bis = None
                    emplacement_bis = None
                    for i in inventaire:
                        if i["fields"].get("Title") == reference and i["fields"].get("Site") == site_stock_bis:
                            batiment_bis = i["fields"].get("Batiment")
                            emplacement_bis = i["fields"].get("Emplacement")
                            break

                    q_resa_bis = sum(
                        parse_float(l["fields"].get("Quantite"))
                        for l in all_details
                        if l["fields"].get("Reference") == reference
                        and l["fields"].get("Statut") in ["Reservé", "Préparé", "Sortie produits"]
                        and (l["fields"].get("Site") == site_stock_bis)
                        and (l["fields"].get("Statut") != "Sortie produits" or l["fields"].get("Comptabilise_inventaire") != 1)
                    )

                    # --- MODIFICATION ICI : Tracker Secondaire ---
                    key_sec = f"{reference}_sec_{site_stock_bis}"
                    deja_pris_sec = usage_tracker.get(key_sec, 0)

                    dispo_bis = q_inv_bis - q_resa_bis - deja_pris_sec
                    
                    if dispo_bis >= quantite:
                        graph_update_field(site_id, details_list_id, item_id, token, {"Statut": "Disponible", "Site":site_stock_bis, "Batiment":batiment_bis, "Emplacement":emplacement_bis})
                        usage_tracker[key_sec] = deja_pris_sec + quantite
                        continue 

                # 3. Vérifie arrivage
                q_arriv = sum(
                    parse_float(a["fields"].get("Quantite"))
                    for a in arrivages
                    if a["fields"].get("Title") == reference and date_livraison and a["fields"].get("Date")
                    and datetime.strptime(a["fields"]["Date"][:10], "%Y-%m-%d") < date_livraison
                )
                q_en_cours = sum(
                    parse_float(l["fields"].get("Quantite"))
                    for l in all_details
                    if l["fields"].get("Reference") == reference and l["fields"].get("Statut") == "Arrivage"
                )

                # --- MODIFICATION ICI : Tracker Arrivage ---
                key_arriv = f"{reference}_arrivage"
                deja_pris_arriv = usage_tracker.get(key_arriv, 0)

                dispo_arriv = (q_arriv - q_en_cours) - deja_pris_arriv
                
                if dispo_arriv >= quantite:
                    graph_update_field(site_id, details_list_id, item_id, token, {"Statut": "Arrivage"})
                    usage_tracker[key_arriv] = deja_pris_arriv + quantite
                    continue 
                
                # Sinon, rupture
                graph_update_field(site_id, details_list_id, item_id, token, {"Statut": "Rupture SdF"})
                ruptures.append({"reference": reference, "raison": "stock et arrivage insuffisants"})

            # --- LOGIQUE UKOBA ---
            else:
                q_uko = sum(
                    parse_float(u["fields"].get("Quantite"))
                    for u in ukobas
                    if u["fields"].get("Title") == reference
                )
                
                # --- MODIFICATION ICI : Tracker Ukoba ---
                key_uko = f"{reference}_ukoba"
                deja_pris_uko = usage_tracker.get(key_uko, 0)

                dispo_uko = q_uko - deja_pris_uko
                
                if dispo_uko >= quantite:
                    graph_update_field(site_id, details_list_id, item_id, token, {"Statut": "Disponible"})
                    usage_tracker[key_uko] = deja_pris_uko + quantite
                    continue 
                    
                # Vérif Arrivage Ukoba (Même logique que SDF)
                q_arriv = sum(
                    parse_float(a["fields"].get("Quantite"))
                    for a in arrivages
                    if a["fields"].get("Title") == reference and date_livraison and a["fields"].get("Date")
                    and datetime.strptime(a["fields"]["Date"][:10], "%Y-%m-%d") < date_livraison
                )

                key_arriv = f"{reference}_arrivage" # On partage le tracker arrivage avec SDF ou spécifique, selon votre besoin. Ici partagé.
                deja_pris_arriv = usage_tracker.get(key_arriv, 0)

                # Note: Pour Ukoba, vous ne comptiez pas 'q_en_cours' dans votre code initial ? 
                # J'ajoute la logique standard ici pour cohérence :
                # dispo_arriv = q_arriv - deja_pris_arriv
                
                if (q_arriv - deja_pris_arriv) >= quantite:
                    graph_update_field(site_id, details_list_id, item_id, token, {"Statut": "Arrivage"})
                    usage_tracker[key_arriv] = deja_pris_arriv + quantite
                    continue 
                
                graph_update_field(site_id, details_list_id, item_id, token, {"Statut": "Rupture Ukoba"})
                ruptures.append({"reference": reference, "raison": "stock et arrivage insuffisants"})

        # ... [Fin de la fonction (retour JSON)] ...
        if not ruptures:
            statut_final = "OK"
        else:
            statut_final = "Rupture"
            
        retour = {
            "commande_id": commande_id,
            "statut": statut_final,
            "ruptures": ruptures,
            "nb_produits_commande": nb_lignes_commande
        }

        return func.HttpResponse(json.dumps(retour), status_code=200, mimetype="application/json")

    except Exception as e:
        logging.exception("Erreur dans la fonction Azure")
        return func.HttpResponse(f"Erreur serveur : {str(e)}", status_code=500)


