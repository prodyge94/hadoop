Pour la PCA on a transformé les données ( réduire l’espace dimensionnel tout en conservant l’essentiel de la variance des données), en conservant uniquement les axes principaux de variation, avant d’entraîner le Random Forest.
Cette démarche assure à la fois performance, l'interprétabilité, et la réduction du risque de sur-apprentissage.
Et surtout de pouvoir visualiser nos données dans un espace en 3D par la suite.

Application de la PCA : 
Avant on  a vectorisé et normalisé nos variables.
	•	on a mis en œuvre de l’analyse en composantes principales avec k = 3 (PC1, PC2, PC3).

	•	Les vecteurs propres ont été extraits pour interpréter la contribution de chaque variable à chaque axe.
	◦	Exemple : PC1 corrélée positivement à account_age_days et verified, et négativement à default_profile.

Visualisation et interprétation
	•	Les données ont été projetées dans l’espace réduit 3D (PC1, PC2, PC3).
	•	Cette projection a permis de visualiser la dispersion des comptes et de repérer des tendances de regroupement entre bots et humains.