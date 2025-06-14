from pyspark.sql import SparkSession

# Créer une session Spark
spark = SparkSession.builder.appName("ExplorationTwitter").getOrCreate()

# Charger le fichier nettoyé depuis HDFS
df = spark.read.option("header", True).csv("hdfs://localhost:9000/twitter/twitter_cleaned")

print(df.columns)

df.show(5)
