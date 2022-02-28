from pyspark.sql.types import StructType


INGESTED_PACKET = StructType() \
    .add("packet", "string") \
    .add("packet_id", "integer") \
    .add("ingestion_id", "integer") \
    .add("user_id", "integer") \
    .add("type", "string")


TRACK_INFO = StructType() \
    .add("title", "string") \
    .add("description", "string") \
    .add("album", "string") \
    .add("album_id", "integer")


ALBUM_INFO = StructType() \
    .add("copyright", "string") \
    .add("year", "string") \
    .add("composer", "string")


TRACK_META = StructType() \
    .add("track_duration", "string") \
    .add("singer", "string") \
    .add("genre", "string")


TRANSFORMED_PACKET = StructType() \
    .add("title", "string") \
    .add("description", "string") \
    .add("album", "string") \
    .add("album_id", "integer") \
    .add("packet_id", "integer") \
    .add("ingestion_id", "integer") \
    .add("user_id", "integer") \
    .add("album_info", "string") \
    .add("track_meta", "string")
