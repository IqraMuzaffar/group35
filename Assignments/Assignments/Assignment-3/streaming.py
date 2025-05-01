from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StringType, IntegerType, DoubleType

# Create Spark Session
spark = SparkSession.builder \
    .appName("Real-Time Traffic Monitoring") \
    .getOrCreate()

# Define Schema for incoming Kafka data
schema = StructType() \
    .add("sensor_id", StringType()) \
    .add("timestamp", StringType()) \
    .add("vehicle_count", IntegerType()) \
    .add("average_speed", DoubleType()) \
    .add("congestion_level", StringType())

# Read streaming data from Kafka topic 'traffic_data'
traffic_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "traffic_data") \
    .load()

# Convert Kafka value from binary to string and parse JSON
traffic_json_df = traffic_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json("value", schema).alias("data")) \
    .select("data.*")

# ✅ Data Quality Checks
traffic_json_df = traffic_json_df.dropna(subset=["sensor_id", "timestamp"]).filter(col("vehicle_count") >= 0).filter(col("average_speed") > 0).dropDuplicates(["sensor_id", "timestamp"])  # Remove duplicate records

# 🧠 Real-Time Traffic Analysis Tasks

# 1. Compute Real-Time Traffic Volume per Sensor (every 5 min)
traffic_agg_df = traffic_json_df \
    .groupBy(window("timestamp", "5 minutes"), "sensor_id") \
    .agg(sum("vehicle_count").alias("total_count"))

# 2. Detect Congestion Hotspots in Real Time
congestion_df = traffic_json_df \
    .filter(col("congestion_level") == "HIGH") \
    .groupBy("sensor_id") \
    .count() \
    .filter(col("count") >= 3)

# 3. Calculate Average Speed per Sensor (10-minute rolling window)
avg_speed_df = traffic_json_df \
    .groupBy(window("timestamp", "10 minutes"), "sensor_id") \
    .agg(avg("average_speed").alias("avg_speed"))

# 4. Identify Sudden Speed Drops (Anomaly Detection)
speed_drop_df = traffic_json_df \
    .withColumn("prev_speed", lag("average_speed").over(Window.partitionBy("sensor_id").orderBy("timestamp"))) \
    .withColumn("speed_drop", when((col("prev_speed") - col("average_speed")) / col("prev_speed") > 0.5, 1).otherwise(0)) \
    .filter(col("speed_drop") == 1)

# 5. Find Top 3 Busiest Sensors in the Last 30 Minutes
busiest_sensors_df = traffic_json_df \
    .groupBy(window("timestamp", "30 minutes"), "sensor_id") \
    .agg(sum("vehicle_count").alias("total_count")) \
    .orderBy(col("total_count").desc()) \
    .limit(3)

# ✅ Write Aggregated Results to Kafka
traffic_agg_df.selectExpr("to_json(struct(*)) AS value") \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "traffic_analysis") \
    .option("checkpointLocation", "/tmp/kafka-checkpoint") \
    .outputMode("update") \
    .start()

# ✅ Write Congestion Hotspots to Kafka
congestion_df.selectExpr("to_json(struct(*)) AS value") \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "congestion_alerts") \
    .option("checkpointLocation", "/tmp/congestion-checkpoint") \
    .outputMode("update") \
    .start()

# ✅ Write Average Speed Results to Kafka
avg_speed_df.selectExpr("to_json(struct(*)) AS value") \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "avg_speed_analysis") \
    .option("checkpointLocation", "/tmp/avg-speed-checkpoint") \
    .outputMode("update") \
    .start()

# ✅ Write Busiest Sensors to Kafka
busiest_sensors_df.selectExpr("to_json(struct(*)) AS value") \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "busiest_sensors") \
    .option("checkpointLocation", "/tmp/busiest-sensors-checkpoint") \
    .outputMode("complete") \
    .start()

# Await termination to keep the streaming job alive
spark.streams.awaitAnyTermination()
