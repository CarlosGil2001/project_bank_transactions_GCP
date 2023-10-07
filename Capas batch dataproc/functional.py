from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, upper
# Create Spark session
spark = SparkSession \
    .builder \
    .master('yarn') \
    .appName('spark-functional') \
    .getOrCreate()

df_risk_credit = spark.read.parquet("gs://ingenieriadatos-392001/datalake/curated/riskcredit/")

df_risk_credit.write.mode("Overwrite").parquet("gs://ingenieriadatos-392001/datalake/functional/riskcredit/")