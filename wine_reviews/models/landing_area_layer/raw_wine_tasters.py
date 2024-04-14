# Databricks notebook source
# MAGIC %run "../../setup/variables"

# COMMAND ----------

spark.conf.set('spark.sql.files.ignoreMissingFiles', True)

# COMMAND ----------

import datetime

from pyspark.sql.types import StringType, IntegerType, FloatType, StructField, StructType, TimestampType
from pyspark.sql.functions import current_date, col, when, lit, udf

# COMMAND ----------

spark.conf.set('spark.sql.sources.partitionOverwriteMode', 'dynamic')

# COMMAND ----------

import datetime

dbutils.widgets.text('ingest_date', '')
ingest_date = dbutils.widgets.get('ingest_date') or datetime.date.today().strftime("%Y-%m-%d")
if ingest_date == '-':
    full_refresh = True
else:
    full_refresh = False

# COMMAND ----------

wines_schema = StructType(fields=[
    StructField('country', StringType()),
    StructField('description', StringType()),
    StructField('designation', StringType()),
    StructField('taster_twitter_handle', StringType()),
    StructField('points', IntegerType()),
    StructField('price', FloatType()),
    StructField('province', StringType()),
    StructField('region_1', StringType()),
    StructField('region_2', StringType()),
    StructField('variety', StringType()),
    StructField('winery', StringType()),
    StructField('load_timestamp', TimestampType()),
])

# COMMAND ----------

if full_refresh:
    ingestion_path = f'{INGESTION_AREA_LAYER}/json/'
else:
    ingestion_path = f'{INGESTION_AREA_LAYER}/json/{ingest_date}/*.json*'

# COMMAND ----------

wines_df = spark.read\
                .format('json')\
                .schema(wines_schema)\
                .option("recursiveFileLookup", "true")\
                .load(ingestion_path)

# COMMAND ----------

wines_df = wines_df.withColumn(
    'ingest_date', lit(ingest_date)
)

# COMMAND ----------

if not full_refresh and spark._jsparkSession.catalog().tableExists('landing_area_layer.raw_wine_tasters'):
    wines_df.write.mode('overwrite').insertInto('landing_area_layer.raw_wine_tasters')
else:
    wines_df.write\
        .format('parquet')\
        .partitionBy('ingest_date')\
        .mode('overwrite')\
        .saveAsTable('landing_area_layer.raw_wine_tasters')

