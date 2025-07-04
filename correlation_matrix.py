from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.stat import Correlation
import pandas as pd

spark = SparkSession.builder.appName("CorrelationMatrix").getOrCreate()

# Load the cleaned twitter dataset from HDFS
# The dataset should be available at hdfs://localhost:9000/twitter/twitter_cleaned
# with a header row and infered schema for proper column types

df = (
    spark.read.option("header", True)
    .option("inferSchema", True)
    .csv("hdfs://localhost:9000/twitter/twitter_cleaned")
)

# Identify numeric and boolean columns
numeric_types = {"double", "int", "bigint", "float", "long", "short"}
boolean_cols = [name for name, dtype in df.dtypes if dtype == "boolean"]
numeric_cols = [name for name, dtype in df.dtypes if dtype in numeric_types]

# Convert boolean columns to integers
for b in boolean_cols:
    df = df.withColumn(b, col(b).cast("int"))

numeric_cols += boolean_cols

# Select only numeric columns
df_numeric = df.select(*numeric_cols)

# Assemble features vector for correlation computation
assembler = VectorAssembler(inputCols=df_numeric.columns, outputCol="features")
df_vector = assembler.transform(df_numeric).select("features")

# Compute the Pearson correlation matrix
corr_matrix = Correlation.corr(df_vector, "features", "pearson").collect()[0][0]

# Convert to Pandas DataFrame for easy CSV export
corr_array = corr_matrix.toArray().tolist()
correlation_df = pd.DataFrame(corr_array, columns=df_numeric.columns, index=df_numeric.columns)
correlation_df.to_csv("correlation_matrix.csv")

spark.stop()
