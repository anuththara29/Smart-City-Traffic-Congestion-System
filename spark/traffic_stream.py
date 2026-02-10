from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.appName("TrafficStreaming").getOrCreate()

df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "traffic-data") \
    .option("startingOffsets", "latest") \
    .load()

schema = StructType([
    StructField("sensor_id", StringType()),
    StructField("timestamp", StringType()),
    StructField("vehicle_count", IntegerType()),
    StructField("avg_speed", IntegerType())
])

json_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("event_time", to_timestamp("timestamp"))

# Alerts
alerts = json_df.filter(col("avg_speed") < 10)

alert_query = alerts.writeStream \
    .format("console") \
    .outputMode("append") \
    .start()

# Window aggregation (Congestion Index)
windowed = json_df \
    .withWatermark("event_time", "1 minute") \
    .groupBy(
        window(col("event_time"), "5 minutes"),
        col("sensor_id")
    ) \
    .agg(
        avg("avg_speed").alias("avg_speed"),
        sum("vehicle_count").alias("total_vehicles")
    )

# Storage
storage_query = windowed.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", "data/traffic_parquet") \
    .option("checkpointLocation", "data/checkpoint") \
    .start()

spark.streams.awaitAnyTermination()
