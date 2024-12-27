from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, explode
import logging
from pyspark.sql.types import StringType, StructType, StructField, FloatType, ArrayType
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(name)s %(message)s',
    handlers=[logging.StreamHandler()]
)

logger = logging.getLogger("SparkKafkaIntegration")

# Create Spark session
spark = SparkSession \
    .builder \
    .appName("Kafka Streaming to CSV") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3") \
    .getOrCreate()

# Kafka stream reading
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "stock_prices") \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .load()

# Data parsing: Kafka messages are in key-value format (both are binary)
df_parsed = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

# Define schema for the stock data (assuming each record contains JSON)
schema = StructType([
    StructField("symbol", StringType(), True),
    StructField("data", StringType(), True)  # The `data` field is expected to be a JSON string
])

# Parse the 'value' field to JSON and create a structured DataFrame
df_parsed_json = df_parsed.withColumn("json_data", from_json(col("value"), schema))

df_flattened = df_parsed_json.select(
    col("json_data.symbol"),
    col("json_data.data").alias("data_json")
)

# Define schema for the nested 'data' field inside `data_json` (using ArrayType and StructType)
data_schema = ArrayType(StructType([
    StructField("index", StringType(), True),
    StructField("Datetime", StringType(), True),
    StructField("Open", FloatType(), True),
    StructField("High", FloatType(), True),
    StructField("Low", FloatType(), True),
    StructField("Close", FloatType(), True),
    StructField("Volume", FloatType(), True),
    StructField("Dividends", FloatType(), True),
    StructField("Stock Splits", FloatType(), True)
]))

# Convert the 'data' field (a JSON string) to a list of records
df_data_parsed = df_flattened.withColumn("data_parsed", from_json(col("data_json"), data_schema))

# Use `explode` to flatten the array of records into separate rows
df_exploded = df_data_parsed.withColumn("data_exploded", explode(col("data_parsed")))

# Flatten the exploded data
df_final = df_exploded.select(
    col("symbol"),
    col("data_exploded.Datetime"),
    col("data_exploded.Open"),
    col("data_exploded.High"),
    col("data_exploded.Low"),
    col("data_exploded.Close"),
    col("data_exploded.Volume")
)

# Write the final DataFrame to CSV (or any other output format like console for testing)
current_date = datetime.now().strftime("%Y-%m-%d")  # Format the current date as yyyy-mm-dd
output_path = f"/tmp/kafka_to_csv_output/{current_date}"

# Write the final DataFrame to CSV (or any other output format like console for testing)
query = df_final \
    .writeStream \
    .outputMode("append") \
    .format("csv") \
    .option("header", "true") \
    .option("checkpointLocation", "/tmp/kafka_checkpoint") \
    .option("path", output_path) \
    .start()

query.awaitTermination()

