import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, avg
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType

KAFKA_BOOTSTRAP_SERVERS = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
KAFKA_TOPIC = os.environ["KAFKA_TOPIC"]
WINDOW_DURATION = os.environ["WINDOW_DURATION"]
STARTING_OFFSETS = os.environ["STARTING_OFFSETS"]
CHECKPOINT_DIR = os.environ["CHECKPOINT_DIR"]

spark = SparkSession.builder.appName("UrbanSensors").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

raw = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", STARTING_OFFSETS)
    .load()
)

schema = StructType([
    StructField("sensor_id", StringType()),
    StructField("temperature", DoubleType()),
    StructField("humidity", DoubleType()),
    StructField("air_quality_index", IntegerType()),
    StructField("timestamp", StringType()),
])

parsed = (
    raw
    .select(from_json(col("value").cast("string"), schema).alias("data"))
    .select("data.*")
    .withColumn("timestamp", col("timestamp").cast("timestamp"))
)

resultado = (
    parsed
    .withWatermark("timestamp", "2 minutes")
    .groupBy(window(col("timestamp"), WINDOW_DURATION), col("sensor_id"))
    .agg(
        avg("temperature").alias("avg_temperature"),
        avg("air_quality_index").alias("avg_air_quality_index"),
    )
)

query = (
    resultado.writeStream
    .outputMode("update")
    .format("console")
    .option("truncate", "false")
    .option("checkpointLocation", CHECKPOINT_DIR)
    .trigger(processingTime="10 seconds")
    .start()
)

query.awaitTermination()