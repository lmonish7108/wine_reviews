# wine_reviews
A Pyspark project in Databricks
<img width="832" alt="image" src="https://github.com/lmonish7108/wine_reviews/assets/44014424/5c3f1fc3-3db6-4b8b-8a1c-c32d8239336e">

Assuming AWS as client data storage
Azure Data factory to pull data from AWS S3 and store in ADLS
Databricks to bring data from ADLS into Unity Catalog
Raw Data will land in Landing Area
Raw data will then be falttened and transformed in to Staging Area
Business views will be created in Semantic area
Azure Data factory to pull data from Semantic area and push it to Power BI


# Step 1: Move data securely from S3 to ADLS
Referring the architecture pattern: https://learn.microsoft.com/en-us/azure/data-factory/data-migration-guidance-s3-azure-storage
