# Databricks notebook source
# MAGIC %md
# MAGIC #### Top 3 wines of each variety and price range in all countries 

# COMMAND ----------

# MAGIC %run "../../setup/variables"

# COMMAND ----------

wines_df = spark.read\
  .format('parquet')\
  .load(f'{STAGING_AREA_LAYER}/stg_wines')

# COMMAND ----------

from pyspark.sql.functions import  sum, col, count, desc, rank, asc

# COMMAND ----------

wines_df_grouped = wines_df.groupBy('country', 'price_range', 'variety').agg(
    sum('points').alias('total_points'), 
    count('*').alias('total_ratings')
)
wines_df_grouped = wines_df_grouped.filter('total_points > 0 and total_ratings > 0 and country is not null')

# COMMAND ----------

from pyspark.sql.window import Window

# COMMAND ----------

wine_rank_spec = Window.partitionBy('country', 'variety').orderBy(desc('total_points'), desc('total_ratings'))

wine_ranked_df = wines_df_grouped.withColumn('rank', rank().over(wine_rank_spec))

# COMMAND ----------

top_3_wines_df = wine_ranked_df.filter('rank <= 3').orderBy(asc('rank'), asc('country'))

# COMMAND ----------

top_3_wines_df.write\
    .format('parquet')\
    .mode('overwrite')\
    .partitionBy('country')\
    .saveAsTable('semantic_area_layer.top_3_wines_variety_by_country')
