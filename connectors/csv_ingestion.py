import os
import sys
import argparse
from uuid import uuid4
from pyspark.sql.functions import struct, to_json, lit, udf
from pyspark.sql.types import StringType
from pyspark.sql import SparkSession, Row
sys.path.append(os.getcwd())

from base.sources.csv_file_stream import CSVFileStream
from base.sinks.delta_table import DeltaTable
from base.common.spark_session import SparkContextSession

# construct input arguments parser
parser = argparse.ArgumentParser()
parser.add_argument("--app_name", help="name of the app.")
parser.add_argument("--input_file_path", help="Delta Path from which data to be imported.")
parser.add_argument("--output_delta_path", help="Delta Path where validated data is to be stored.")
parser.add_argument("--ingestion_id", help="Unique id for the digestion.")
parser.add_argument("--product_identifier_column", help="Unique id for the identifying product.")
parser.add_argument("--user_id", help="User meta info.")
cmd_args = parser.parse_args()

# Create spark context
session_context = SparkContextSession(cmd_args.app_name)
service = session_context.get_service()

# Trigger CSV import
input_stream = CSVFileStream(
    service, is_streaming=False, file_path=cmd_args.input_file_path or "static/assets"
)
input_stream = input_stream.load()

# Transformation based on schema of delta table
ingestion_id = cmd_args.ingestion_id or str(uuid4())
user_id = cmd_args.user_id or ''
product_identifier_column = cmd_args.product_identifier_column or 'id'

# Data transformation - Dump all the input payload in stringified json format along with additional info
input_stream = input_stream.withColumn("packet", to_json(struct('*')))
input_stream_columns = input_stream.columns
id_exists = product_identifier_column in input_stream_columns
default_column_list = ['packet']

if id_exists:
    default_column_list.append(product_identifier_column) 

updated_input_stream = input_stream[default_column_list]
updated_input_stream = updated_input_stream. \
    withColumn("ingestion_id", lit(ingestion_id)). \
    withColumn("type", lit("csv_import")). \
    withColumn("user_id", lit(user_id))

if not id_exists:
    uuidUdf = udf(lambda : str(uuid4()), StringType())
    updated_input_stream = updated_input_stream.withColumn('packet_id', uuidUdf())
else:
    updated_input_stream = updated_input_stream.withColumnRenamed(product_identifier_column, 'packet_id')

# Write the raw data transformed into schema to delta table
write_stream = DeltaTable(updated_input_stream, is_delta=True, is_streaming=False, file_path=cmd_args.output_delta_path or "target/ingest/delta")
write_stream.start()
