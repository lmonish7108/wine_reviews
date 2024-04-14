# Databricks notebook source
# MAGIC %run "../../setup/variables"

# COMMAND ----------

import datetime

# COMMAND ----------

dbutils.widgets.text('ingest_date', '')
ingest_date = dbutils.widgets.get('ingest_date') or datetime.date.today().strftime("%Y-%m-%d")
if ingest_date == '-':
    full_refresh = True
else:
    full_refresh = False

# COMMAND ----------

if full_refresh:
    landing_area_path = f'{LANDING_AREA_LAYER}/raw_wines/'
else:
    landing_area_path = f'{LANDING_AREA_LAYER}/raw_wines/ingest_date={ingest_date}/'

wines_df = spark.read\
            .format('parquet')\
            .load(landing_area_path)

# COMMAND ----------

import random
from pyspark.sql.functions import col, when, lit, udf
from pyspark.sql.types import IntegerType

# COMMAND ----------

@udf(returnType=IntegerType())
def generate_price(p):
    if p is not None:
        return int(p)
    return random.randint(4,55)

# COMMAND ----------

wines_df = wines_df.withColumn(
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

if not full_refresh and spark._jsparkSession.catalog().tableExists('staging_area_layer.stg_wines'):
    wines_df.write.mode('overwrite').insertInto('staging_area_layer.stg_wines')
else:
    wines_df.write\
        .format('parquet')\
        .partitionBy('ingest_date')\
        .mode('overwrite')\
        .saveAsTable('staging_area_layer.stg_wines')
