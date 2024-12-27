import os
import json
import time
import requests
import sys
from kafka import KafkaProducer
from dotenv import load_dotenv
import yfinance as yf

API_KEY = os.getenv('API_KEY')
SYMBOL = 'AAPL'  # You can change this to any stock symbol
KAFKA_BROKER = 'localhost:9092'
TOPIC = 'stock_prices'

# Initialize Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)


def fetch_stock_data(symbol):
    # Fetch data using yfinance
    stock = yf.Ticker(symbol)
    hist = stock.history(period="1d", interval="60m")  # 1-minute interval data for the day
    # Convert the DataFrame to JSON'
    hist = hist.reset_index()
    hist['Datetime'] = hist['Datetime'].dt.strftime('%Y-%m-%d %H:%M:%S%z')
    data = hist.reset_index().to_dict(orient='records')
    return data


def send_to_kafka(data, symbol):
    payload = {
        'symbol': symbol,
        'data': data,
    }
    producer.send(TOPIC, payload)
    print(f"Sent data to {TOPIC}: {payload}")

def main(symbol):
    while True:
        stock_data = fetch_stock_data(symbol)
        if stock_data:
            send_to_kafka(stock_data, symbol)
        else:
            print(f"Waiting for valid data for symbol: {symbol}")
        time.sleep(60)  # Fetch data every 60 seconds

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python stock_producer.py <STOCK_SYMBOL>")
        sys.exit(1)
    stock_symbol = sys.argv[1]
    main(stock_symbol)
