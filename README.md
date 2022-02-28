# data_lake_pipeline
Data lake pipeline based on APACHE Spark Streaming following the similar approach of delta lake (bronze, silver, gold) by databricks

# BRONZE TABLE:
    - Contains all the ingested packet without any transformation along with ingestion information

# SILVER TABLE:
    - Contains a transformed version of bronze table payload with necessary information for gold table

# GOLD TABLE:
    - Primary Data store
    - Search Store
