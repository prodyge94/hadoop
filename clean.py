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
    "has_description", when(col("description").isNotNull(), True).otherwise(False)
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

#df_final.write.option("header", True).mode("overwrite").csv("hdfs://localhost:9000/twitter/twitter_cleaned")
df.select("account_type").distinct().show(100)

df_final.show(5)
