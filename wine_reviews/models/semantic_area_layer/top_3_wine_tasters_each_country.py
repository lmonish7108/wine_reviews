# Databricks notebook source
# MAGIC %md
# MAGIC #### Top 3 wine tasters in each country

# COMMAND ----------

# MAGIC %run "../../setup/variables"

# COMMAND ----------

wines_df = spark.read\
  .format('parquet')\
  .load(f'{STAGING_AREA_LAYER}/stg_wine_tasters')

# COMMAND ----------

from pyspark.sql.functions import  sum, col, count, desc, rank, asc

# COMMAND ----------

wines_df_grouped = wines_df.groupBy('taster_twitter_handle', 'country').agg(
    count('*').alias('total_reviews')
).filter('country IS NOT NULL and taster_twitter_handle IS NOT NULL')

# COMMAND ----------

from pyspark.sql.window import Window

# COMMAND ----------

wine_rank_spec = Window.partitionBy('country').orderBy(desc('total_reviews'))

wine_ranked_df = wines_df_grouped.withColumn('rank', rank().over(wine_rank_spec))

# COMMAND ----------

top_3_wines_df = wine_ranked_df.filter('rank <= 3').orderBy(asc('rank'), asc('country')).orderBy(asc('country'))

# COMMAND ----------

top_3_wines_df.write\
    .format('parquet')\
    .mode('overwrite')\
    .saveAsTable('semantic_area_layer.top_3_wine_tasters_each_country')
