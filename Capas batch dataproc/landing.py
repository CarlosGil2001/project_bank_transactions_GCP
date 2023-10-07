from pyspark.sql import SparkSession
# Create Spark session
spark = SparkSession \
    .builder \
    .master('yarn') \
    .appName('spark-bigquery—demo') \
    .getOrCreate()

# Citties
df_cities = spark.read.format("csv").option("header","true").load("gs://ingenieriadatos-392001/datalake/workload/cities.csv")

df_cities.write.mode("Overwrite").parquet("gs://ingenieriadatos-392001/datalake/landing/cities/")

# Countries
df_countries = spark.read.format("csv").option("header","true").load("gs://ingenieriadatos-392001/datalake/workload/countries.csv")

df_countries.write.mode("Overwrite").parquet("gs://ingenieriadatos-392001/datalake/landing/countries/")

# Risk Credit
df_riskcredit = df_countries = spark.read.format("csv").option("header","true").load("gs://ingenieriadatos-392001/datalake/workload/riskcredit.csv")

df_riskcredit.write.mode("Overwrite").parquet("gs://ingenieriadatos-392001/datalake/landing/riskcredit/")