# Databricks notebook source
# MAGIC %run "../utils/mount_storage_util"

# COMMAND ----------

CONTAINERS = ['ingestion', 'landing', 'staging', 'semantic']

# COMMAND ----------

for container in CONTAINERS:
    mount_container(container)

# COMMAND ----------

spark.conf.set('spark.stagingAreaDBLocation', f"/mnt/{dbutils.secrets.get('winereviews-secrets', 'STORAGEACCOUNT')}/staging")
spark.conf.set('spark.semanticAreaDBLocation', f"/mnt/{dbutils.secrets.get('winereviews-secrets', 'STORAGEACCOUNT')}/semantic")
spark.conf.set('spark.landingAreaDBLocation', f"/mnt/{dbutils.secrets.get('winereviews-secrets', 'STORAGEACCOUNT')}/landing")

# COMMAND ----------

# MAGIC %sql
# MAGIC create database if not exists landing_area_layer
# MAGIC location '${spark.landingAreaDBLocation}';
# MAGIC create database if not exists staging_area_layer
# MAGIC location '${spark.stagingAreaDBLocation}';
# MAGIC create database if not exists semantic_area_layer
# MAGIC location '${spark.semanticAreaDBLocation}';
# MAGIC
