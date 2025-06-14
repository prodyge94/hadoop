from pyspark.sql import SparkSession
from pyspark.sql.functions import avg
#  coefficient de pierson, afficher une acp
spark = SparkSession.builder.appName("AnalyseTwitterComplete").getOrCreate()

# Chargement du fichier nettoyé
df = spark.read.option("header", True).csv("hdfs://localhost:9000/twitter/twitter_cleaned")

# Analyse 1 : Répartition humain vs bots
account_type = df.groupBy("account_type").count()
account_type.toPandas().to_csv("account_type.csv", index=False)

# Analyse 2 : Moyennes comportements
comportements = df.groupBy("account_type").agg(
    avg("followers_count").alias("moyenne_followers"),
    avg("friends_count").alias("moyenne_friends"),
    avg("statuses_count").alias("moyenne_statuses"),
    avg("favourites_count").alias("moyenne_favourites")
)
comportements.toPandas().to_csv("comportements_moyens.csv", index=False)

# Analyse 3 : Tweets quotidiens et âge moyen
tweets_age = df.groupBy("account_type").agg(
    avg("average_tweets_per_day").alias("tweets_par_jour"),
    avg("account_age_days").alias("age_moyen_compte")
)
tweets_age.toPandas().to_csv("tweets_age.csv", index=False)

# Analyse 4 : Présence de description
has_description = df.groupBy("account_type", "has_description").count()
has_description.toPandas().to_csv("has_description.csv", index=False)

# Analyse 5 : Langues principales PAR type de compte
langues = df.groupBy("account_type", "lang").count().orderBy("account_type", "count", ascending=False)
langues.toPandas().to_csv("langues_par_type.csv", index=False)

# Analyse 6 : Comptes vérifiés
verified = df.groupBy("account_type", "verified").count()
verified.toPandas().to_csv("verified.csv", index=False)

# Analyse 7 : Géolocalisation activée
geo = df.groupBy("account_type", "geo_enabled").count()
geo.toPandas().to_csv("geo_enabled.csv", index=False)

spark.stop()
