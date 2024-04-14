# Databricks notebook source
STORAGEACCOUNT = dbutils.secrets.get('winereviews-secrets', 'STORAGEACCOUNT')

INGESTION_AREA_LAYER = f'/mnt/{STORAGEACCOUNT}/ingestion'
LANDING_AREA_LAYER = f'/mnt/{STORAGEACCOUNT}/landing'
STAGING_AREA_LAYER = f'/mnt/{STORAGEACCOUNT}/staging'
SEMANTIC_AREA_LAYER = f'/mnt/{STORAGEACCOUNT}/semantic'
