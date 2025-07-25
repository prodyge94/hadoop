GROUPE 8 - 3IABD2
BEN AMMAR Mehdy, GUILLOT Nicolas, LEROUX Dimitri, VU CONG David, YUKSEL Ozkan


###Sélection du dataset : 

On a commencé par rechercher un dataset sur kaggle qui pouvait correspondre à ce qu'on voulait. Suite à ces recherches, on a décidé de choisirce dataset : https://www.kaggle.com/datasets/danieltreiman/twitter-human-bots-dataset/data

###Nettoyage du dataset : 

Ce dataset contenait 23 colonnes à l'origine et permet de différencier des comptes bots/humains provenant de twitter. malgré tout ce dataset pose quelques problèmes et doit donc être modifié pour être exploité. 
C'est pourquoi nous avons dans un premier temps créer le script clean.py : ce script permet de sélectionner pour nous uniquement les colonnes que nous avons jugés utiles. Nous avons aussi créé une nouvelle colonne hasDescription pour lister les comptes avec ou sans description. Nous avons ensuite converti les données stockées sous forme de chaines de caractères true/false en nombre flottant 0.0/1.0. Puis suite à ces modifications, on recréé un nouveau fichier twitter_cleaned sur hdfs, correspondant au nouveau fichier ui va être utilisé comme dataset.

Par la suite dans notre fichier d'application, nous avons remarqué que le dataset était fortement déséquilibré, c'est pour ça que lors du chargement de ce fichier, nous équilibrons dans un premier temps les 2 classes humans/bot.

###Visualisation des données via PowerBI : 

Nous avons utilisé PowerQuery pour importer les différents fichiers csv et utiliser les outils mis à disposition sur PowerBI afin de pouvoir créer les différents types de graphes, permettant de visualiser les champs de notre dataset et les différences de comportements entre un Humain et un Bot.

###Matrice de corrélation Pearson

On a décidé de faire une matrice de corrélation pour visualiser la corrélation entre chaque colonnes. On a remarqué que le status_count et average_tweets_per_day était très simialire donc on a décidé d'exclure ce dernier dans nos expérimentations.

###PCA
Pour la PCA on a transformé les données ( réduire l’espace dimensionnel tout en conservant l’essentiel de la variance des données), en conservant uniquement les axes principaux de variation, avant d’entraîner le Random Forest. Cette démarche assure à la fois performance, l'interprétabilité, et la réduction du risque de sur-apprentissage. Et surtout de pouvoir visualiser nos données dans un espace en 3D par la suite.

Avant on a vectorisé et normalisé nos variables.
Application de la PCA :

on a mis en œuvre de l’analyse en composantes principales avec k = 3 (PC1, PC2, PC3). Les vecteurs propres ont été extraits pour interpréter la contribution de chaque variable à chaque axe. Exemple : PC1 corrélée positivement à account_age_days et verified, et négativement à default_profile.
Visualisation et interprétation
Les données ont été projetées dans l’espace réduit 3D (PC1, PC2, PC3). Cette projection a permis de visualiser la dispersion des comptes et de repérer des tendances de regroupement entre bots et humains.

###Random Forest

on a utilisé random forest pour faire du ML sur notre dataset équilibré. on a séparé le dataset en 2 parties : 80% train, 20%test. EN testant plusieurs configurations de numtrees et de maxdepth, on a décidé de garder numtrees = 200 et maxdepth = 6 pour une accuracy finale d'environ 77%.
On a ensuite testé ce modèle de ML avec des cas unitaires générés à la main et le résultat a été dans al plupart des cas correct.