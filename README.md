# POCSDF

## CommandeVerification

1. La Prise d'informations (Le Contexte)
Comme ses grandes sœurs, la fonction reçoit le numéro de la commande (commande_id). Elle s'authentifie silencieusement, lit l'en-tête de la commande pour identifier le Site de Stockage Principal (Site_Stock), le Site Secondaire (Site_Stock_second) et la Date de livraison. Ensuite, elle télécharge la liste de tous les produits demandés.

2. Le Chargement Intelligent (Data & Historique)
Pour éviter de saturer SharePoint :

La fonction télécharge le catalogue des produits, l'inventaire complet, les arrivages, et la liste d'inventaire externe (ukobalistid).

L'optimisation : Elle liste toutes les références uniques de la commande, identifie celles d'origine interne ("SDF"), et ne télécharge l'historique des réservations que pour ces références précises. C'est un gain de temps énorme.

3. L'Analyse Ligne par Ligne (Le Tracker Virtuel)
Le cœur de la fonction est son usage_tracker. Ce dictionnaire en mémoire permet à la fonction de retenir ce qu'elle vient "virtuellement" de distribuer. Si la commande demande 2 chaises sur la ligne 1, et 2 chaises sur la ligne 3, le tracker garantit qu'on vérifiera s'il y a bien 4 chaises en stock au total, et non pas 2 fois 2 chaises !

La fonction traite ensuite chaque ligne selon la famille de produit :

Famille A : Origine Interne (SDF)
Elle applique une logique de vérification en cascade :

Test 1 : Le Site Principal. Elle calcule le disponible (Stock Physique - Réservations existantes - Déjà pris dans cette boucle).
👉 S'il y en a assez : La ligne passe au statut "Disponible". La fonction trouve le Bâtiment et l'Emplacement exacts dans l'inventaire et les inscrit sur la ligne.

Test 2 : Le Site Secondaire. Si le Site Principal est à sec, elle refait le même calcul sur le Site Secondaire (s'il existe).
👉 S'il y en a assez : La ligne passe en "Disponible" avec la localisation du Site Secondaire.

Test 3 : Les Arrivages. Si les stocks physiques sont vides, elle cherche un arrivage prévu avant la date de livraison de la commande (en soustrayant ce qui est déjà réservé sur cet arrivage).
👉 Si c'est bon : La ligne passe au statut "Arrivage".

Échec : Si aucun de ces tests ne passe, la ligne est mise à jour avec le statut "Rupture SdF" et est ajoutée à la liste rouge des ruptures.

Famille B : Origine Externe (Ukoba / Fournisseurs)
Pour les autres produits, la logique est similaire mais tape dans une liste différente :

Test 1 : Le Stock Ukoba. Elle calcule le disponible dans la base ukobas (Stock total Ukoba - Déjà pris dans cette boucle).
👉 S'il y en a assez : La ligne passe en "Disponible".

Test 2 : Les Arrivages Ukoba. Même logique que pour SDF : y a-t-il une livraison prévue avant notre date butoir ?
👉 Si oui : La ligne passe en "Arrivage".

Échec : Sinon, statut "Rupture Ukoba" et ajout à la liste rouge.

4. Le Bilan
Une fois la boucle terminée, la fonction génère son rapport final. Contrairement à CommandeValidation, elle ne modifie pas l'en-tête principal de la commande. Elle se contente de renvoyer :

Un statut global ("OK" ou "Rupture").

Le nombre de produits traités.

Le détail précis de toutes les ruptures trouvées.

## CommandeVerificationMateriel

1. Récupération de la date cible (La "Photo" temporelle)
La fonction ne regarde pas le stock au hasard, elle a besoin d'une date précise.

Elle va d'abord chercher ta commande grâce au commande_id fourni en paramètre.

Sur cette commande, elle récupère le fameux Aff_ID.

Elle interroge ensuite la liste des affaires/événements (affaireevtslistid) avec cet Aff_ID pour extraire la date exacte de l'événement (Date_evt).

2. État des lieux du stock physique (Le "Total")
La fonction télécharge l'intégralité de ton catalogue de matériel depuis materielstocklistid.

Elle construit un dictionnaire en mémoire (très rapide à lire) qui associe chaque référence de matériel à sa quantité physique totale (ex: {"Tente 3x3": 10, "Chaise": 50}).

3. Calcul des réservations concurrentes (Les "Validés")
C'est ici qu'intervient le filtre temporel.

Elle interroge la liste des réservations (materielreservationlistid) pour récupérer uniquement les lignes dont le statut est "Validé".

Elle trie ces lignes validées pour ne garder que celles qui tombent exactement à la même date que ton événement.

Elle additionne ensuite les quantités de ces lignes par référence pour savoir combien de pièces sont déjà "bloquées" pour ce jour-là.

4. Calcul du disponible (La "Soustraction")
En mémoire, le script fait une soustraction mathématique très simple pour chaque référence :
[Quantité Totale en Stock] - [Quantité Réservée à cette date] = Quantité Disponible réelle.

5. Mise à jour de ta commande (L'Écriture)
Maintenant que la fonction connaît la vérité sur le stock disponible à cette date :

Elle récupère toutes les lignes de matériel qui appartiennent à ta commande actuelle (via le filtre CMD_ID eq '{commande_id}').

Pour chacune de ces lignes, elle regarde la référence demandée, pioche le chiffre de disponibilité calculé à l'étape 4, et met à jour la colonne qte_dispo directement dans SharePoint.

Enfin, elle renvoie un message de succès avec le nombre de lignes mises à jour.


## CommandeImportation

1. Réception et "Prise de Contexte"
Le script reçoit le fichier encodé (Base64) et le décode pour pouvoir le lire.

Il s'authentifie silencieusement auprès de Microsoft Graph.

Il va chercher l'en-tête de la commande dans SharePoint pour récupérer les informations cruciales : le Type d'import, le CMD_ID, l'AFF_ID, et surtout, il va fouiller dans les événements pour récupérer la Date de l'événement (nécessaire pour les réservations de matériel).

2. Le Nettoyage (La "Purge")
Avant d'écrire quoi que ce soit, le script s'assure qu'on repart d'une page blanche. Il supprime toutes les anciennes lignes de produits (cmddetailslistid) rattachées à ce CMD_ID. Ça permet à l'utilisateur de réimporter un fichier corrigé sans créer de doublons.

3. L'Aiguillage (Le "Switch" par type de fichier)
Le script lit le fichier différemment selon son origine :

Prestation / Grossiste (Excel) : * Contrôle de sécurité : Il compare le montant total du fichier Excel avec les tarifs de ta base de données. S'il y a une différence (et que le "bypass" n'est pas activé), le script s'arrête net et renvoie le statut maj pour forcer l'utilisateur à valider les prix.

Si tout est bon, il lit les onglets de données en ignorant les lignes vides de fin de tableau.

Finale3D / Pyromotion (CSV) : Il lit simplement le fichier texte en formatant chaque ligne en produit.

FWSIM (CSV) : Il lit le fichier, mais ajoute un triage intelligent :

Si la ligne contient le mot "MAT", elle est rangée dans le panier nouveaux_materiels (avec la date de l'événement).

Sinon, elle est rangée dans le panier nouveaux_details (les produits classiques).

4. Le Contrôle Douane (Vérification Matériel)
Si le script a trouvé du matériel dans le fichier (ex: FWSIM), il fige tout et fait une vérification de sécurité :

Il extrait toutes les références uniques de matériel demandées.

Il interroge la liste materiellistid (ton catalogue) pour vérifier qu'elles existent toutes.

S'il en manque ne serait-ce qu'une seule : l'import est annulé, aucune ligne n'est créée, et le script renvoie le statut materiel_manquant avec le nom exact de la référence coupable pour alerter l'utilisateur.

5. Le Rangement (Le Tri avant insertion)
Pour s'assurer que tes exports futurs soient toujours jolis et lisibles :

Il trie la liste des produits grâce à un algorithme "intelligent" (pour que la ligne 2 soit bien placée avant la ligne 10, et non après la ligne 1 comme le ferait un tri alphabétique basique).

Il trie le matériel par ordre alphabétique de référence.

6. La Livraison (Insertion Massive / Batch)
C'est la phase d'écriture. Au lieu d'envoyer les lignes une par une à SharePoint (ce qui prendrait des heures et ferait planter le serveur) :

Le script fait des "paquets" (Batch) de 20 lignes.

Il envoie un paquet de 20 produits d'un coup, puis le suivant, jusqu'à épuisement.

Il fait exactement la même chose pour le matériel (vers la liste materielreservationlistid).

7. Clôture de l'opération
Une fois toutes les requêtes terminées avec succès, le script met à jour le statut de l'import à "oui" dans la liste principale, et renvoie un rapport complet à Power Automate (nombre de lignes de produits créées, nombre de matériels créés).


## CommandeReception 

1. Prise d'informations (Le Bordereau)
La fonction reçoit une "demande de réception" depuis Power Automate contenant :

Le numéro de la commande (commande_id).

L'endroit exact où la marchandise est physiquement réceptionnée (site_recept, batiment_recept, emplacement_recept).

Elle s'authentifie, récupère les informations de l'en-tête de la commande (notamment les sites de stockage théoriques Site_Stock et Site_Stock_second) et télécharge toutes les lignes de détails (les produits).

2. Le Chargement (Le "Scan" de l'entrepôt)
Pour prendre des décisions éclairées, le script télécharge massivement les données "monde" en arrière-plan :

Le catalogue complet des produits.

L'inventaire total.

Les historiques de réservations uniquement pour les produits "SDF" présents dans la commande (optimisation pour éviter de télécharger des milliers de lignes inutiles).

3. L'Inspection ligne par ligne (Le "Pointage")
Le script parcourt chaque produit de la commande et regarde son "Origine". Il y a deux grandes familles :

Famille A : Les produits externes (Ukoba ou autres)
Pas de contrôle de stock stricte : Si le produit ne vient pas de "SDF", le script part du principe que si le fournisseur l'a livré, on l'accepte sans discuter les chiffres de l'inventaire théorique.

Mise à jour : Le statut passe à "Préparé", et surtout, la fonction met à jour l'adresse physique théorique (Site, Batiment, Emplacement) ET l'adresse physique de préparation (Site_prepa, etc.) avec les valeurs saisies par l'utilisateur lors de la réception (site_recept, etc.).

Famille B : Les produits internes ("SDF")
C'est là qu'a lieu la gymnastique. Le script regarde le statut de la ligne de commande :

Si le produit est en "Rupture SdF" : Le script donne une seconde chance au produit. Peut-être qu'un arrivage ou qu'une remise en stock a eu lieu depuis la dernière vérification !

Il recompte le stock physique (inventaire) sur le Site Principal et soustrait les réservations.

Si la quantité est finalement suffisante, le produit est "sauvé" : son statut passe à "Préparé" avec les adresses de *_prepa renseignées.

S'il n'y a toujours pas assez de stock, il vérifie le Site Secondaire (si renseigné). S'il y en a assez, le produit est "sauvé" là-bas.

Si le stock est toujours insuffisant après les deux vérifications, le produit est ajouté à la liste des ruptures.

Si le produit n'est PAS en rupture (il était déjà OK) : Le script ne refait pas de calculs inutiles, il passe directement le statut de la ligne à "Préparé" et remplit les champs de destination *_prepa.

4. Bilan et Clôture
Une fois toutes les lignes passées en revue :

L'en-tête principal de la commande passe au statut "Réceptionné".

La fonction renvoie un rapport détaillé de l'opération : un statut global (soit "Validé", soit "Validé (Rupture SdF)" si des lignes n'ont pas pu être sauvées) et la liste des références problématiques.


## CommandeValidation

1. L'Étude du dossier (Prise de contexte)
La fonction reçoit le numéro de la commande. Elle commence par lire l'en-tête pour savoir :

Vers quel Site Principal (Site_Stock) la commande est censée taper.

Quel est le Site de Secours (Site_Stock_second) au cas où.

Quelle est la Date de livraison (pour calculer si on peut compter sur des arrivages futurs).
Elle télécharge ensuite toutes les lignes (produits) demandées dans cette commande.

2. Le Renseignement ultra-optimisé (Data & Historique)
Pour ne pas faire exploser les serveurs de Microsoft, la fonction est très intelligente sur ce qu'elle télécharge :

Elle charge le catalogue (produits), l'inventaire physique total, et les arrivages prévus.

Le coup de génie : Elle regarde les références de la commande, identifie uniquement celles qui sont fabriquées en interne (Origine == "SDF"), et ne télécharge l'historique des réservations que pour ces références précises. C'est un gain de temps et de performance énorme.

3. Le Tri par "Famille" (L'Aiguillage)
La fonction analyse chaque ligne de la commande et regarde l'Origine du produit :

Si le produit est "Non-SDF" (ex: Ukoba ou fournisseur externe) : La fonction ne se pose aucune question de stock. Elle passe directement le statut de la ligne à "Commandé". C'est logique : on va l'acheter au fournisseur.

4. L'Algorithme de la "Cascade" (Pour les produits SDF)
Pour le matériel interne (SDF), la fonction applique une logique en cascade (si le plan A échoue, on passe au plan B, etc.) :

Plan A : Le Site Principal. Elle compte le stock physique sur le Site_Stock, soustrait ce qui est déjà réservé pour d'autres clients, et soustrait ce qu'elle vient juste de réserver dans la boucle actuelle (grâce à un usage_tracker qui évite de donner deux fois la même chaise !).
👉 S'il y en a assez : La ligne passe en "Reservé" et on lui assigne le Bâtiment et l'Emplacement du site principal.

Plan B : Le Site Secondaire. Si le plan A échoue, elle fait exactement le même calcul (Stock - Réservé) mais sur le Site_Stock_second.
👉 S'il y en a assez : La ligne passe en "Reservé" et on lui assigne la localisation du site secondaire.

Plan C : Les Arrivages Futurs. Si le produit n'est physiquement nulle part, la fonction regarde la liste des arrivages. Elle cherche un arrivage prévu avant la Date_livraison de la commande.
👉 Si un camion doit arriver à temps : La ligne passe au statut "Arrivage".

Plan D : L'Échec (La Rupture). Si le plan A, B et C échouent, il n'y a pas de miracle : le produit est officiellement manquant. La fonction l'ajoute à une liste noire (ruptures). (Note : dans ton code, l'écriture du statut "Rupture SdF" sur la ligne est actuellement commentée, mais l'information est bien remontée).

5. Le Coup de Tampon Final
Une fois que toutes les lignes ont été traitées, la fonction met à jour l'en-tête de la commande globale :

Si aucune rupture n'a été détectée ➡️ La commande passe au statut "Validé".

Si au moins une rupture a été détectée ➡️ La commande passe au statut "Validé (Rupture SdF)".
Enfin, elle renvoie la liste exacte des références en rupture à Power Automate (pour que tu puisses, par exemple, envoyer un e-mail d'alerte aux acheteurs).



