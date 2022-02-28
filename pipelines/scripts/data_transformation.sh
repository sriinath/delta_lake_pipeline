. .env/data_lake_pipeline/bin/activate
pip install -r requirements.txt
spark-submit --packages io.delta:delta-core_2.12:1.1.0 --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" pipelines/data_transformation.py
