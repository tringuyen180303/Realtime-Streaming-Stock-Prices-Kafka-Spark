# import os
# import json
# from datetime import datetime, timedelta
# from airflow import DAG
# from airflow.operators.python import PythonOperator
# from airflow.operators.empty import EmptyOperator
# from airflow.operators.bash import BashOperator

# # Define Kafka broker and topic
# KAFKA_BROKER = ':9092'
# TOPIC = 'stock_prices'

# # Define the symbol you want to track
# SYMBOL = 'AAPL'  # Example stock symbol

# # Define the path to the Spark and Streamlit scripts
# SPARK_SCRIPT = '/src/spark.py'
# STREAMLIT_SCRIPT = '/src/stream_lit.py'

# # Function to fetch stock data and send it to Kafka
# def fetch_and_send_stock_data(symbol):
#     import yfinance as yf
#     from kafka import KafkaProducer
#     import json
#     # Fetch data using yfinance
#     stock = yf.Ticker(symbol)
#     hist = stock.history(period="1d", interval="60m")  # 1-hour interval data for the day
#     hist = hist.reset_index()
#     hist['Datetime'] = hist['Datetime'].dt.strftime('%Y-%m-%d %H:%M:%S%z')
#     data = hist.reset_index().to_dict(orient='records')

#     # Prepare Kafka Producer
#     producer = KafkaProducer(bootstrap_servers=['broker:29092'], value_serializer=lambda v: json.dumps(v).encode('utf-8'))

#     # Send data to Kafka
#     payload = {'symbol': symbol, 'data': data}
#     producer.send(TOPIC, payload)
#     producer.close()

# # Function to trigger Spark job to process the Kafka data and output to CSV
# def trigger_spark_processing():
#     from subprocess import Popen, PIPE
#     # Run the Spark script
#     process = Popen(['python3', SPARK_SCRIPT], stdout=PIPE, stderr=PIPE)
#     stdout, stderr = process.communicate()

#     if process.returncode != 0:
#         print(f"Error in Spark processing: {stderr}")
#     else:
#         print(f"Spark processing completed: {stdout}")

# # Define the Airflow DAG
# default_args = {
#     'owner': 'airflow',
#     'start_date': datetime(2024, 12, 27),
#     'retries': 1,
#     'retry_delay': timedelta(minutes=5),
# }


# with DAG(
#     'stock_price_processing',
#     default_args=default_args,
#     description='A simple DAG to fetch stock data, process it with Spark, and visualize with Streamlit',
#     schedule_interval=timedelta(days=1),  # Runs every 1 minute
#     catchup=False
# ) as dag:

#     # Task 1: Fetch and send stock data
#     fetch_stock_data_task = PythonOperator(
#         task_id='fetch_and_send_stock_data',
#         python_callable=fetch_and_send_stock_data,
#         op_args=[SYMBOL],
#     )

#     # Task 2: Trigger Spark processing
#     spark_processing_task = PythonOperator(
#         task_id='trigger_spark_processing',
#         python_callable=trigger_spark_processing,
#     )

#     # Set task dependencies
#     fetch_stock_data_task >> spark_processing_task

import os
import json
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator

# Define Kafka broker and topic
KAFKA_BROKER = 'broker:29092'  # Adjust as needed, e.g. 'localhost:9092'
TOPIC = 'stock_prices'

# Here is a sample subset of SP100 symbols. Expand as needed:
SP100_SYMBOLS = [
    "AAPL", "MSFT", "AMZN", "GOOGL", "GOOG", "BRK-B", "NVDA", "TSLA",
    "META", "JPM", "V", "JNJ", "WMT", "PG", "MA", "HD", "UNH", "BAC",
    "XOM", "DIS", "PFE", "CVX", "KO", "CSCO", "PEP", "ABT", "NFLX",
    "T", "VZ", "INTC", "ADBE", "MRK", "CRM", "CMCSA", "ORCL", "COST",
    "ABBV", "WFC", "ACN", "AVGO", "NEE", "QCOM", "TXN", "LIN", "PM",
    # ... add more SP100 symbols as needed ...
]

SPARK_SCRIPT = '/src/spark.py'        # Adjust path to your Spark script
STREAMLIT_SCRIPT = '/src/stream_lit.py'  # Adjust path to your Streamlit script


def fetch_and_send_stock_data(symbols):
    """
    Loops over a list of symbols, fetches the latest stock data via yfinance,
    and publishes each symbol's data to Kafka.
    """
    import yfinance as yf
    from kafka import KafkaProducer

    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    for symbol in symbols:
        stock = yf.Ticker(symbol)
        # Example: last 1 day of data in 60-minute intervals
        hist = stock.history(period="1d", interval="60m")

        # Convert Datetime to string
        hist.reset_index(inplace=True)
        hist['Datetime'] = hist['Datetime'].dt.strftime('%Y-%m-%d %H:%M:%S%z')

        # Create a JSON-friendly structure
        data = hist.to_dict(orient='records')
        payload = {'symbol': symbol, 'data': data}

        producer.send(TOPIC, payload)
        print(f"Sent data for {symbol} to topic '{TOPIC}'.")

    producer.close()


def trigger_spark_processing():
    """
    Runs the Spark script to process all data from Kafka and writes it to CSV (or any sink).
    """
    from subprocess import Popen, PIPE

    print("Starting Spark processing...")
    process = Popen(['python3', SPARK_SCRIPT], stdout=PIPE, stderr=PIPE)
    stdout, stderr = process.communicate()

    if process.returncode != 0:
        print(f"Error in Spark processing: {stderr.decode()}")
    else:
        print(f"Spark processing completed successfully: {stdout.decode()}")


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 12, 27),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'stock_price_processing_sp100',
    default_args=default_args,
    description='Fetch SP100 stocks, send to Kafka, then run Spark processing',
    schedule_interval=timedelta(minutes=1),  # runs every minute
    catchup=False
) as dag:

    fetch_stock_data_task = PythonOperator(
        task_id='fetch_and_send_stock_data',
        python_callable=fetch_and_send_stock_data,
        op_args=[SP100_SYMBOLS],  # Passing the entire list
    )

    spark_processing_task = PythonOperator(
        task_id='trigger_spark_processing',
        python_callable=trigger_spark_processing,
    )

    fetch_stock_data_task >> spark_processing_task
