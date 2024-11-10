from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType
from sentence_transformers import SentenceTransformer
from elasticsearch import Elasticsearch
import json
import numpy as np

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("StockDataProcessor") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Define Kafka source
kafka_bootstrap_servers = "kafka:9092"
stock_topic = "stock_prices"

# Define the schema of the incoming JSON data
stock_schema = StructType([
    StructField("timestamp", StringType()),
    StructField("symbol", StringType()),
    StructField("open", FloatType()),
    StructField("high", FloatType()),
    StructField("low", FloatType()),
    StructField("close", FloatType()),
    StructField("volume", IntegerType())
])

# Read data from Kafka
stock_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", stock_topic) \
    .option("startingOffsets", "latest") \
    .load()

# Convert the binary 'value' column to string
stock_values = stock_df.selectExpr("CAST(value AS STRING) as json_str")

# Parse the JSON data
parsed_df = stock_values.select(from_json(col("json_str"), stock_schema).alias("data")).select("data.*")

# Initialize the embedding model
model = SentenceTransformer('all-MiniLM-L6-v2')

# Function to generate embeddings
def generate_embedding(row):
    doc_text = f"Timestamp: {row['timestamp']} Symbol: {row['symbol']} Open: {row['open']} High: {row['high']} Low: {row['low']} Close: {row['close']} Volume: {row['volume']}"
    embedding = model.encode(doc_text).tolist()
    return embedding

# Register the function as a UDF
from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, FloatType

embedding_udf = udf(generate_embedding, ArrayType(FloatType()))

# Apply the UDF to create embeddings
embedded_df = parsed_df.withColumn("embedding", embedding_udf(col("timestamp")))

# Initialize Elasticsearch client
es = Elasticsearch(hosts=["http://elasticsearch:9200"])

# Function to write each batch to Elasticsearch
def write_to_es(batch_df, batch_id):
    records = batch_df.collect()
    for row in records:
        doc = {
            "timestamp": row["timestamp"],
            "symbol": row["symbol"],
            "open": row["open"],
            "high": row["high"],
            "low": row["low"],
            "close": row["close"],
            "volume": row["volume"],
            "embedding": row["embedding"]
        }
        doc_id = f"{row['symbol']}_{row['timestamp']}"
        es.index(index="stock_prices", id=doc_id, body=doc)

# Write stream to Elasticsearch
query = embedded_df.writeStream \
    .foreachBatch(write_to_es) \
    .outputMode("append") \
    .start()

query.awaitTermination()
