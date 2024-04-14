# Demo DataLake project on Azure Databricks

## TechStack
- Azure Databricks: ETL
- Azure DataFactory: Orchestration
- Azure KeyVault: Saving Secrets like S3 access keys, Databricks access keys
- Azure Entra: Using Entra to access ADLS in Databricks using Service Principal

### Key points:
- All Containers are mounted to Databricks workspace
- Datafactory access to trigger Databricks notebook is through Databricks PAT
- Keyvault is connected to Databricks via Databricks secrets API
- Keyvault for other Azure service is connected using Managed Identity

### ToDos:
- Stream data processing using Spark Structured streaming

### Step 1: Ingestion
<img width="713" alt="image" src="https://github.com/lmonish7108/wine_reviews/assets/44014424/ae0d5111-4a06-477e-a215-9be1a258b864">

1. Batch-Data is currently stored in AWS Cloud, so first we have to bring data to Azure Storage Blob
2. Created Azure Data Factory pipeline to pull data every day incrementally (based on ingest_date)
3. Data is pulled from S3 bucket and stored in ADLS ingestion container

### Step 2: Data Pipeline
<img width="611" alt="image" src="https://github.com/lmonish7108/wine_reviews/assets/44014424/46b8800f-d66b-43e0-877a-261cb2112065">

1. Data is pushed to Landing_area_layer where data can be flattened and timestamps can be added
2. Transformation: Data is pushed to Staging_area_layer with required transformations
3. Business views are created in Semantic Layer
<img width="1002" alt="image" src="https://github.com/lmonish7108/wine_reviews/assets/44014424/8a7ec87d-5883-4894-895a-dc9365f30db2">





