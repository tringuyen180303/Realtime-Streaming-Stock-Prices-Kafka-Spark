import os
import json
import time
import requests
from kafka import KafkaProducer
from dotenv import load_dotenv

API_KEY = os.getenv('API_KEY')
SYMBOL = 'AAPL'  # You can change this to any stock symbol
KAFKA_BROKER = 'localhost:9092'
TOPIC = 'stock_prices'

# Initialize Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def fetch_stock_data():
    url = f'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol={SYMBOL}&interval=1min&apikey={API_KEY}'
    response = requests.get(url)
    data = response.json()
    return data

def send_to_kafka(data):
    producer.send(TOPIC, data)
    print(f"Sent data to {TOPIC}: {data}")

if __name__ == "__main__":
    while True:
        stock_data = fetch_stock_data()
        send_to_kafka(stock_data)
        time.sleep(60)  # Wait for 60 seconds before fetching new data
