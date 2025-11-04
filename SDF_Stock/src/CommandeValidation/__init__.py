import logging
import azure.functions as func
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
import requests
import urllib.parse
import json
from datetime import datetime

# --------- CONFIG ------------
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
        filter_param = urllib.parse.quote(filter_expr, safe="=()/")  # ne pas échapper les () ni eq
        base_url += f"&$filter={filter_param}"

    results = []
    url = base_url

    while url:
        res = requests.get(url, headers=headers)
        res.raise_for_status()
        data = res.json()
        results.extend(data.get("value", []))
        url = data.get("@odata.nextLink")

    return results




def get_graph_token(tenant_id, client_id, client_secret):
    url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
    data = {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret,
        "scope": "https://graph.microsoft.com/.default"
    }
    response = requests.post(url, data=data)
    logging.info(f"Get token Response : {response.content}")
    return response.json().get("access_token")

app = func.FunctionApp()

def parse_float(value):
    try:
        return float(value)
    except:
        return 0

def main(req: func.HttpRequest) -> func.HttpResponse:
    try:
        body = req.get_json()
        commande_id = body.get("commande_id")
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

        # --- Données
        try:
            commande_items = graph_filtered_items(site_id, commandes_list_id, token, f"fields/CMD_ID eq {commande_id}")
            if not commande_items:
                return func.HttpResponse("Commande introuvable", status_code=404)
            commande = commande_items[0]["fields"]


        except requests.exceptions.HTTPError:
            return func.HttpResponse("Commande introuvable", status_code=404)

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
            reference = d.get("Title")
            item_id = detail["id"] 
            quantite = parse_float(d.get("Quantite"))
            statut = d.get("Statut")
            # graph_update_field(site_id, details_list_id, item_id, token, {"Statut_prepa": "OK"})

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

                # Vérifie site principal
                q_inv = sum(
                    parse_float(i["fields"].get("Quantite"))
                    for i in inventaire
                    if i["fields"].get("Title") == reference and i["fields"].get("Site") == site_stock
                )
                q_resa = sum(
                    parse_float(l["fields"].get("Quantite"))
                    for l in all_details
                    if l["fields"].get("Title") == reference
                    and l["fields"].get("Statut") in ["Reservé", "Préparé", "Sortie produits"]
                    and (l["fields"].get("Site") == site_stock)
                    and (l["fields"].get("Statut") != "Sortie produits" or l["fields"].get("Comptabilise_inventaire") != 1)
                )
                dispo = q_inv - q_resa
                if dispo >= quantite:
                    continue  # Produit validé dans site principal
                logging.info("   Produit éligible au contrôle de stock (origine SDF)")
                logging.info("   Site principal : %s", site_stock)
                logging.info("   q_inv (stock) site principal : %s", q_inv)
                logging.info("   q_resa (réservé) site principal : %s", q_resa)
                logging.info("   dispo = q_inv - q_resa : %s", dispo)
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
                        if l["fields"].get("Title") == reference
                        and l["fields"].get("Statut") in ["Reservé", "Préparé", "Sortie produits"]
                        and (l["fields"].get("Site") == site_stock_bis)
                        and (l["fields"].get("Statut") != "Sortie produits" or l["fields"].get("Comptabilise_inventaire") != 1)
                    )
                    dispo_bis = q_inv_bis - q_resa_bis
                    logging.info("   ➤ Site secondaire : %s", site_stock_bis)
                    logging.info("   ➤ q_inv_bis (stock) : %s", q_inv_bis)
                    logging.info("   ➤ q_resa_bis (réservé) : %s", q_resa_bis)
                    logging.info("   ➤ dispo_bis = q_inv_bis - q_resa_bis : %s", dispo_bis)
                    if dispo_bis >= quantite:
                        continue  # Produit validé dans site secondaire

                # Vérifie arrivage

                q_arriv = sum(
                    parse_float(a["fields"].get("Quantite"))
                    for a in arrivages
                    if a["fields"].get("Title") == reference and date_livraison and a["fields"].get("Date")
                    and datetime.strptime(a["fields"]["Date"][:10], "%Y-%m-%d") < date_livraison
                )
                q_en_cours = sum(
                    parse_float(l["fields"].get("Quantite"))
                    for l in all_details
                    if l["fields"].get("Title") == reference and l["fields"].get("Statut") == "Arrivage"
                )
                if (q_arriv - q_en_cours) >= quantite:
                    continue  # Arrivage prévu avant la date
                logging.info("   ➤ q_arriv (prévision livrée avant date) : %s", q_arriv)
                logging.info("   ➤ q_en_cours (déjà réservée en 'Arrivage') : %s", q_en_cours)
                logging.info("   ➤ arrivage dispo = q_arriv - q_en_cours : %s", q_arriv - q_en_cours)
                # Sinon, rupture
                ruptures.append({"reference": reference, "raison": "stock et arrivage insuffisants"})

            else:
                logging.info("   ➤ Produit non SDF – pas de contrôle de stock (considéré disponible)")
                # Produit Ukoba : on considère "Commandé", jamais rupture
                continue

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


