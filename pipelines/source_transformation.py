import os
import sys
import argparse
import numpy as np

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, schema_of_json, udf, when
from pyspark.sql.types import BooleanType
sys.path.append(os.getcwd())

from base.sources.delta_stream import DeltaStream
from base.sinks.delta_table import DeltaTable
from base.common.spark_session import SparkContextSession
from pipelines.models import TRACK_INFO, ALBUM_INFO, TRACK_META

# construct input arguments parser
parser = argparse.ArgumentParser()
parser.add_argument("--app_name", help="name of the app.")
parser.add_argument("--input_delta_path", help="Delta Path from which data to be imported.")
parser.add_argument("--output_delta_path", help="Delta Path where validated data is to be stored.")
parser.add_argument("--failure_delta_path", help="Delta Path where to pusj invalid packet.")

cmd_args = parser.parse_args()

# Create spark context
session_context = SparkContextSession(cmd_args.app_name)
service = session_context.get_service()

# Trigger delta read (silver table)
input_stream = DeltaStream(
    service, file_path=cmd_args.input_delta_path or "target/ingest/delta",
    is_streaming=False
)
input_stream = input_stream.load()

# Transform the raw ingested records into streamlined format (Bronze Table)
transformed_input_stream = input_stream.select(
    'packet_id',
    'ingestion_id',
    'user_id',
    from_json(col("packet"), TRACK_INFO).alias("track_info"),
    from_json(col("packet"), TRACK_META).alias("track_meta"),
    from_json(col("packet"), ALBUM_INFO).alias("album_info")
).select(
    'packet_id',
    'ingestion_id',
    'user_id',
    'track_meta',
    'album_info',
    'track_info.*'
)

# Validate if input packet is valid
transformed_input_stream = transformed_input_stream.withColumn(
    'is_valid',
    when(col('packet_id').isNull() | col('ingestion_id').isNull() | col('title').isNull(), False) \
    .otherwise(True)
)

# Filter invalid packets and write to failure delta table
failed_packet_stream = transformed_input_stream[transformed_input_stream['is_valid'] == False]
failure_write_stream = DeltaTable(failed_packet_stream, is_delta=True, is_streaming=False, file_path=cmd_args.failure_delta_path or "target/failed/delta")
failure_write_stream = failure_write_stream.start()

# Write the streamlined records into bronze table
transformed_input_stream = transformed_input_stream[transformed_input_stream['is_valid'] == True]
transformed_input_stream = transformed_input_stream.drop('is_valid')
write_stream = DeltaTable(transformed_input_stream, is_delta=True, is_streaming=False, file_path=cmd_args.output_delta_path or "target/output/delta")
write_stream = write_stream.start()

# write_stream.awaitTermination(100)
# failure_write_stream.awaitTermination(10)
