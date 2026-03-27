# POCSDF

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
