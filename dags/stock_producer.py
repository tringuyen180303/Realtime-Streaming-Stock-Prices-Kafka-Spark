import os
import json
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator

# Define Kafka broker and topic
KAFKA_BROKER = ':9092'
TOPIC = 'stock_prices'

# Define the symbol you want to track
SYMBOL = 'AAPL'  # Example stock symbol

# Define the path to the Spark and Streamlit scripts
SPARK_SCRIPT = '/src/spark.py'
STREAMLIT_SCRIPT = '/src/stream_lit.py'

# Function to fetch stock data and send it to Kafka
def fetch_and_send_stock_data(symbol):
    import yfinance as yf
    from kafka import KafkaProducer
    import json
    # Fetch data using yfinance
    stock = yf.Ticker(symbol)
    hist = stock.history(period="1d", interval="60m")  # 1-hour interval data for the day
    hist = hist.reset_index()
    hist['Datetime'] = hist['Datetime'].dt.strftime('%Y-%m-%d %H:%M:%S%z')
    data = hist.reset_index().to_dict(orient='records')

    # Prepare Kafka Producer
    producer = KafkaProducer(bootstrap_servers=['broker:29092'], value_serializer=lambda v: json.dumps(v).encode('utf-8'))

    # Send data to Kafka
    payload = {'symbol': symbol, 'data': data}
    producer.send(TOPIC, payload)
    producer.close()

# Function to trigger Spark job to process the Kafka data and output to CSV
def trigger_spark_processing():
    from subprocess import Popen, PIPE
    # Run the Spark script
    process = Popen(['python3', SPARK_SCRIPT], stdout=PIPE, stderr=PIPE)
    stdout, stderr = process.communicate()

    if process.returncode != 0:
        print(f"Error in Spark processing: {stderr}")
    else:
        print(f"Spark processing completed: {stdout}")

# Define the Airflow DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 12, 27),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


with DAG(
    'stock_price_processing',
    default_args=default_args,
    description='A simple DAG to fetch stock data, process it with Spark, and visualize with Streamlit',
    schedule_interval=timedelta(days=1),  # Runs every 1 minute
    catchup=False
) as dag:

    # Task 1: Fetch and send stock data
    fetch_stock_data_task = PythonOperator(
        task_id='fetch_and_send_stock_data',
        python_callable=fetch_and_send_stock_data,
        op_args=[SYMBOL],
    )

    # Task 2: Trigger Spark processing
    spark_processing_task = PythonOperator(
        task_id='trigger_spark_processing',
        python_callable=trigger_spark_processing,
    )

    # Set task dependencies
    fetch_stock_data_task >> spark_processing_task