# Databricks notebook source
# MAGIC %run "../../setup/variables"

# COMMAND ----------

import datetime
dbutils.widgets.text('ingest_date', '')
ingest_date = dbutils.widgets.get('ingest_date') or datetime.date.today().strftime("%Y-%m-%d")
if ingest_date == '-':
    full_refresh = True
else:
    full_refresh = False

# COMMAND ----------

if full_refresh:
    landing_area_path = f'{LANDING_AREA_LAYER}/raw_wine_tasters/'
else:
    landing_area_path = f'{LANDING_AREA_LAYER}/raw_wine_tasters/ingest_date={ingest_date}/'

wines_tasters_df = spark.read\
                    .format('parquet')\
                    .load(landing_area_path)

# COMMAND ----------

import random
from pyspark.sql.functions import col, when, lit, udf
from pyspark.sql.types import IntegerType

# COMMAND ----------

@udf(returnType=IntegerType())
def generate_price(p):
    return p or random.randint(4,15)

# COMMAND ----------

wines_tasters_df = wines_tasters_df.withColumn(
    'price', generate_price("price")
).withColumn(
    "price_range", 
    when(col('price') <= 9, lit('value'))\
    .when((col('price') > 9) & (col('price') <= 17), lit('popular'))\
    .when((col('price') > 17) & (col('price') <= 30), lit('premium'))\
    .when((col('price') > 30) & (col('price') <= 50), lit('super_premium'))\
    .when((col('price') > 50) & (col('price') <= 70), lit('luxury'))\
    .when(col('price') > 70, lit('super_luxury'))\
    .otherwise('other')
).withColumn('ingest_date', lit(ingest_date))

# COMMAND ----------

if not full_refresh and spark._jsparkSession.catalog().tableExists('staging_area_layer.stg_wine_tasters'):
    wines_tasters_df.write.mode('overwrite').insertInto('staging_area_layer.stg_wine_tasters')
else:
    wines_tasters_df.write\
        .format('parquet')\
        .partitionBy('ingest_date')\
        .mode('overwrite')\
        .saveAsTable('staging_area_layer.stg_wine_tasters')
