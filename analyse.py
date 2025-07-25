from pyspark.sql import SparkSession
from pyspark.sql.functions import avg

# Init Spark
spark = SparkSession.builder.appName("AnalyseTwitterComplete").getOrCreate()

# Chargement du fichier nettoyé
df = spark.read.option("header", True).csv("hdfs://localhost:9000/twitter/twitter_cleaned")

# Cast des colonnes nécessaires pour les moyennes
df = df.withColumn("followers_count", df["followers_count"].cast("double")) \
       .withColumn("friends_count", df["friends_count"].cast("double")) \
       .withColumn("statuses_count", df["statuses_count"].cast("double")) \
       .withColumn("favourites_count", df["favourites_count"].cast("double")) \
       .withColumn("average_tweets_per_day", df["average_tweets_per_day"].cast("double")) \
       .withColumn("account_age_days", df["account_age_days"].cast("double"))

# Analyse 1 : Répartition humain vs bots
df.groupBy("account_type").count() \
  .write.mode("overwrite").option("header", True) \
  .csv("hdfs://localhost:9000/twitter/output/account_type")

# Analyse 2 : Moyennes comportements
df.groupBy("account_type").agg(
    avg("followers_count").alias("moyenne_followers"),
    avg("friends_count").alias("moyenne_friends"),
    avg("statuses_count").alias("moyenne_statuses"),
    avg("favourites_count").alias("moyenne_favourites")
).write.mode("overwrite").option("header", True) \
  .csv("hdfs://localhost:9000/twitter/output/comportements_moyens")

# Analyse 3 : Tweets quotidiens et âge moyen
df.groupBy("account_type").agg(
    avg("average_tweets_per_day").alias("tweets_par_jour"),
    avg("account_age_days").alias("age_moyen_compte")
).write.mode("overwrite").option("header", True) \
  .csv("hdfs://localhost:9000/twitter/output/tweets_age")

# Analyse 4 : Présence de description
df.groupBy("account_type", "has_description").count() \
  .write.mode("overwrite").option("header", True) \
  .csv("hdfs://localhost:9000/twitter/output/has_description")

# Analyse 5 : Langues principales
df.groupBy("account_type", "lang").count() \
  .orderBy("account_type", "count", ascending=False) \
  .write.mode("overwrite").option("header", True) \
  .csv("hdfs://localhost:9000/twitter/output/langues_par_type")

# Analyse 6 : Comptes vérifiés
df.groupBy("account_type", "verified").count() \
  .write.mode("overwrite").option("header", True) \
  .csv("hdfs://localhost:9000/twitter/output/verified")

# Analyse 7 : Géolocalisation activée
df.groupBy("account_type", "geo_enabled").count() \
  .write.mode("overwrite").option("header", True) \
  .csv("hdfs://localhost:9000/twitter/output/geo_enabled")

spark.stop()
