from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, DecimalType
# Create Spark session
spark = SparkSession \
    .builder \
    .master('yarn') \
    .appName('spark-curated') \
    .getOrCreate()


path_cities = "gs://ingenieriadatos-392001/datalake/landing/cities/"
path_countries = "gs://ingenieriadatos-392001/datalake/landing/countries/"
path_riskcredit = "gs://ingenieriadatos-392001/datalake/landing/riskcredit/"

parquet_schema_cities = StructType([
    StructField("CITY", StringType(), True),
    StructField("COUNTRY", StringType(), True),
    StructField("LATITUDE", DecimalType(16, 9), True),
    StructField("LONGITUDE", DecimalType(16, 9), True)
])

parquet_schema_countries = StructType([
    StructField("COUNTRY", StringType(), True),
    StructField("ALPHA_2", StringType(), True)
])

# Cities clean and set Schema
df_cities = spark.read.parquet(path_cities)

df_cities_format = df_cities.select(
    col("City"),col("Country"),
    col("Latitude").cast("decimal(16,9)").alias("LATITUDE"),
    col("Longitude").cast("decimal(16,9)").alias("LATITUDE"),
)

df_cities_pq = spark.createDataFrame(df_cities_format.rdd, schema=parquet_schema_cities)

df_cities_pq.write.mode("Overwrite").parquet("gs://ingenieriadatos-392001/datalake/curated/cities/")

df_cities_cr = spark.read.parquet("gs://ingenieriadatos-392001/datalake/curated/cities/")

# Paises
df_paises = spark.read.parquet(path_countries)

df_paises_format = df_paises.select(
    col("name"),col("alpha-2")
)

df_paises_pq = spark.createDataFrame(df_paises_format.rdd, schema=parquet_schema_countries)

df_paises_pq.write.mode("Overwrite").parquet("gs://ingenieriadatos-392001/datalake/curated/countries/")

# Risk Credit
df_risk = spark.read.parquet(path_riskcredit)

parquet_schema = StructType([
    StructField("ID", StringType(), True),
    StructField("COMPANYNAME", StringType(), True),
    StructField("TURNOVER_2015", DecimalType(18, 6), True),
    StructField("TURNOVER_2016", DecimalType(18, 6), True),
    StructField("TURNOVER_2017", DecimalType(18, 6), True),
    StructField("TURNOVER_2018", DecimalType(18, 6), True),
    StructField("TURNOVER_2019", DecimalType(18, 6), True),
    StructField("TURNOVER_2020", DecimalType(18, 6), True),
    StructField("EBIT_2015", DecimalType(18, 6), True),
    StructField("EBIT_2016", DecimalType(18, 6), True),
    StructField("EBIT_2017", DecimalType(18, 6), True),
    StructField("EBIT_2018", DecimalType(18, 6), True),
    StructField("EBIT_2019", DecimalType(18, 6), True),
    StructField("EBIT_2020", DecimalType(18, 6), True),
    StructField("PLTAX2015", DecimalType(18, 6), True),
    StructField("PLTAX2016", DecimalType(18, 6), True),
    StructField("PLTAX2017", DecimalType(18, 6), True),
    StructField("PLTAX2018", DecimalType(18, 6), True),
    StructField("PLTAX2019", DecimalType(18, 6), True),
    StructField("PLTAX2020", DecimalType(18, 6), True),
    StructField("MSCORE_2015", DecimalType(18, 6), True),
    StructField("MSCORE_2016", DecimalType(18, 6), True),
    StructField("MSCORE_2017", DecimalType(18, 6), True),
    StructField("MSCORE_2018", DecimalType(18, 6), True),
    StructField("MSCORE_2019", DecimalType(18, 6), True),
    StructField("MSCORE_2020", DecimalType(18, 6), True),
    StructField("REGION", StringType(), True),
    StructField("COUNTRY", StringType(), True),
    StructField("NACE_CODE", StringType(), True),
    StructField("SECTOR_1", StringType(), True),
    StructField("SECTOR_2", StringType(), True),
    StructField("LEVERAGE_2015", DecimalType(18, 6), True),
    StructField("LEVERAGE_2016", DecimalType(18, 6), True),
    StructField("LEVERAGE_2017", DecimalType(18, 6), True),
    StructField("LEVERAGE_2018", DecimalType(18, 6), True),
    StructField("LEVERAGE_2019", DecimalType(18, 6), True),
    StructField("LEVERAGE_2020", DecimalType(18, 6), True),
    StructField("ROE_2015", DecimalType(18, 6), True),
    StructField("ROE_2016", DecimalType(18, 6), True),
    StructField("ROE_2017", DecimalType(18, 6), True),
    StructField("ROE_2018", DecimalType(18, 6), True),
    StructField("ROE_2019", DecimalType(18, 6), True),
    StructField("ROE_2020", DecimalType(18, 6), True),
    StructField("TOTAL ASSET2015", DecimalType(18, 6), True),
    StructField("TOTAL ASSET2016", DecimalType(18, 6), True),
    StructField("TOTAL ASSET2017", DecimalType(18, 6), True),
    StructField("TOTAL ASSET2018", DecimalType(18, 6), True),
    StructField("TOTAL ASSET2019", DecimalType(18, 6), True),
    StructField("TOTAL ASSET2020", DecimalType(18, 6), True),
])

df_risk_casted = df_risk.select(
    col("No").alias("ID"),
    col("Company name").alias("COMPANYNAME"),
    col("`Turnover.2020`").cast("decimal(18, 6)").alias("TURNOVER_2020"),
    col("`Turnover.2019`").cast("decimal(18, 6)").alias("TURNOVER_2019"),
    col("`Turnover.2018`").cast("decimal(18, 6)").alias("TURNOVER_2018"),
    col("`Turnover.2017`").cast("decimal(18, 6)").alias("TURNOVER_2017"),
    col("`Turnover.2016`").cast("decimal(18, 6)").alias("TURNOVER_2016"),
    col("`Turnover.2015`").cast("decimal(18, 6)").alias("TURNOVER_2015"),
    col("`EBIT.2020`").cast("decimal(18, 6)").alias("EBIT_2020"),
    col("`EBIT.2019`").cast("decimal(18, 6)").alias("EBIT_2019"),
    col("`EBIT.2018`").cast("decimal(18, 6)").alias("EBIT_2018"),
    col("`EBIT.2017`").cast("decimal(18, 6)").alias("EBIT_2017"),
    col("`EBIT.2016`").cast("decimal(18, 6)").alias("EBIT_2016"),
    col("`EBIT.2015`").cast("decimal(18, 6)").alias("EBIT_2015"),
    col("`PLTax.2020`").cast("decimal(18, 6)").alias("PLTAX2020"),
    col("`PLTax.2019`").cast("decimal(18, 6)").alias("PLTAX2019"),
    col("`PLTax.2018`").cast("decimal(18, 6)").alias("PLTAX2018"),
    col("`PLTax.2017`").cast("decimal(18, 6)").alias("PLTAX2017"),
    col("`PLTax.2016`").cast("decimal(18, 6)").alias("PLTAX2016"),
    col("`PLTax.2015`").cast("decimal(18, 6)").alias("PLTAX2015"),
    col("`MScore.2020`").cast("decimal(18, 6)").alias("MSCORE_2020"),
    col("`MScore.2019`").cast("decimal(18, 6)").alias("MSCORE_2019"),
    col("`MScore.2018`").cast("decimal(18, 6)").alias("MSCORE_2018"),
    col("`MScore.2017`").cast("decimal(18, 6)").alias("MSCORE_2017"),
    col("`MScore.2016`").cast("decimal(18, 6)").alias("MSCORE_2016"),
    col("`MScore.2015`").cast("decimal(18, 6)").alias("MSCORE_2015"),
    col("Region"),
    col("Country"),
    col("NACE code"),
    col("Sector 1"),
    col("Sector 2"),
    col("`Leverage.2020`").cast("decimal(18, 6)").alias("LEVERAGE_2020"),
    col("`Leverage.2019`").cast("decimal(18, 6)").alias("LEVERAGE_2019"),
    col("`Leverage.2018`").cast("decimal(18, 6)").alias("LEVERAGE_2018"),
    col("`Leverage.2017`").cast("decimal(18, 6)").alias("LEVERAGE_2017"),
    col("`Leverage.2016`").cast("decimal(18, 6)").alias("LEVERAGE_2016"),
    col("`Leverage.2015`").cast("decimal(18, 6)").alias("LEVERAGE_2015"),
    col("`ROE.2020`").cast("decimal(18, 6)").alias("ROE_2020"),
    col("`ROE.2019`").cast("decimal(18, 6)").alias("ROE_2019"),
    col("`ROE.2018`").cast("decimal(18, 6)").alias("ROE_2018"),
    col("`ROE.2017`").cast("decimal(18, 6)").alias("ROE_2017"),
    col("`ROE.2016`").cast("decimal(18, 6)").alias("ROE_2016"),
    col("`ROE.2015`").cast("decimal(18, 6)").alias("ROE_2015"),
    col("`TAsset.2020`").cast("decimal(18, 6)").alias("TASSET_2020"),
    col("`TAsset.2019`").cast("decimal(18, 6)").alias("TASSET_2019"),
    col("`TAsset.2018`").cast("decimal(18, 6)").alias("TASSET_2018"),
    col("`TAsset.2017`").cast("decimal(18, 6)").alias("TASSET_2017"),
    col("`TAsset.2016`").cast("decimal(18, 6)").alias("TASSET_2016"),
    col("`TAsset.2015`").cast("decimal(18, 6)").alias("TASSET_2015")
)


df_risk_pq = spark.createDataFrame(df_risk_casted.rdd, schema=parquet_schema)

df_risk_pq.write.mode("Overwrite").parquet("gs://ingenieriadatos-392001/datalake/curated/riskcredit/")