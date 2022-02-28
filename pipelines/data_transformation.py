import os
import sys
import argparse
import numpy as np

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
sys.path.append(os.getcwd())

from base.sources.delta_stream import DeltaStream
from base.sinks.delta_table import DeltaTable
from base.common.spark_session import SparkContextSession
from pipelines.models import TRANSFORMED_PACKET

# construct input arguments parser
parser = argparse.ArgumentParser()
parser.add_argument("--app_name", help="name of the app.")
parser.add_argument("--input_delta_path", help="Delta Path from which data to be imported.")
parser.add_argument("--output_delta_path", help="Delta Path where validated data is to be stored.")

cmd_args = parser.parse_args()

# Create spark context
session_context = SparkContextSession(cmd_args.app_name)
service = session_context.get_service()

# Trigger delta read (silver table)
input_stream = DeltaStream(
    service, file_path=cmd_args.input_delta_path or "target/output/delta",
    is_streaming=False
)
input_stream = input_stream.load()

# Transform the streamlined packets from bronze table
transformed_input_stream = input_stream.select(
    col('user_id').alias('uploaded_by'),
    '*'
).select(
    'title',
    'description',
    'album',
    'album_id',
    'packet_id',
    'album_info',
    'track_meta',
    'uploaded_by'
)

# Write the streamlined records into gold table (primary data store)
write_stream = DeltaTable(transformed_input_stream, is_delta=True, is_streaming=False, file_path=cmd_args.output_delta_path or "target/data_store/delta")
write_stream = write_stream.start()
# write_stream.awaitTermination(100)
