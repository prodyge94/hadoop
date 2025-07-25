from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

spark = SparkSession.builder.appName("NettoyageTwitter").getOrCreate()

# Lire le CSV depuis HDFS avec l'entête
df = spark.read.option("header", True)\
    .option("delimiter", ",")\
    .option("encoding", "utf-8")\
    .option("multiLine", True)\
    .option("quote", '"')\
    .option("escape", '"')\
    .csv("hdfs://localhost:9000/twitter/twitter_human_bots_dataset.csv")

# Créer la colonne booléenne has_description
df_cleaned = df.withColumn(
    "has_description", when(col("description").isNotNull(), "true").otherwise("false")
)

# Sélectionner uniquement les colonnes utiles
colonnes_utiles = [
    "default_profile",
    "default_profile_image",
    "has_description",
    "favourites_count",
    "followers_count",
    "friends_count",
    "geo_enabled",
    "lang",
    "location",
    "statuses_count",
    "verified",
    "average_tweets_per_day",
    "account_age_days",
    "account_type"
]

df_final = df_cleaned.select(*colonnes_utiles)

# Liste des colonnes booléennes à convertir en 0.0 / 1.0
colonnes_bool = [
    "default_profile",
    "default_profile_image",
    "has_description",
    "geo_enabled",
    "verified"
]

# Conversion en float : true/false → 1.0/0.0
for c in colonnes_bool:
    df_final = df_final.withColumn(
    c,
    when(col(c).isin("true", "True"), 1.0)
    .when(col(c).isin("false", "False"), 0.0)
    .otherwise(None)
)


# Écriture du fichier nettoyé
df_final.write.option("header", True).mode("overwrite")\
    .csv("hdfs://localhost:9000/twitter/twitter_cleaned")

# Vérification
df.select("account_type").distinct().show(100)
df_final.show(5)
