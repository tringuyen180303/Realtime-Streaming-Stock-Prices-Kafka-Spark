# LMM-Stock-Price

This project is using Airflow to retrieve stock values from Yahoo Finance and then utilizing Kafka and Spark to streaming real-time then parse in the csv file. Then using streamlit to visualize the stock prices

---

# Start Docker-compose
To begin, run Docker-compose to stall all the required containers.

```
docker-compose up -d
```


To check if containers running successfuly:
```
docker ps
```
![screenshot](/images/docker_containers.png)

Continuously monitoring Kafka and control center containers as they are very vulnerable.

# Create Topic inside Kafka Container
Kafka need a topic to send the data to so you need to create a topic inside Kafka container

```
docker exec -it <kafka_container_id> bash

kafka-topics --create --topic stock_prices --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```



# Kafka to test if data is retrieved
To test if the data is being sent to Kafka, navigate to kafka directory and run the producer and consumer
```
cd kafka
python3 kafka_producer.py <Stock_Symbol>
python3 kakfa_consumer.py
```


You should see 
```
Sent data to stock_prices: {'symbol': 'MSFT', 'data': [{'index': 0, 'Datetime': '2024-12-27 09:30:00-0500', 'Open': 434.9100036621094, 'High': 435.2200012207031, 'Low': 427.3599853515625, 'Close': 428.3399963378906, 'Volume': 3065756, 'Dividends': 0.0, 'Stock Splits': 0.0}, {'index': 1, 'Datetime': '2024-12-27 10:30:00-0500', 'Open': 428.2300109863281, 'High': 429.75, 'Low': 427.45001220703125, 'Close': 429.68499755859375, 'Volume': 2647876, 'Dividends': 0.0, 'Stock Splits': 0.0}, {'index': 2, 'Datetime': '2024-12-27 11:30:00-0500', 'Open': 429.69000244140625, 'High': 429.9164123535156, 'Low': 426.5249938964844, 'Close': 426.8410949707031, 'Volume': 1582834, 'Dividends': 0.0, 'Stock Splits': 0.0}, {'index': 3, 'Datetime': '2024-12-27 12:30:00-0500', 'Open': 426.8349914550781, 'High': 428.4800109863281, 'Low': 426.3500061035156, 'Close': 428.24249267578125, 'Volume': 982963, 'Dividends': 0.0, 'Stock Splits': 0.0}]}
```


# Visualize and trigger DAG manually
To trigger DAG manually or monitoring airflow, navigate to:

```
localhost:8080
```

Since I already provided entrypoint.sh that creating the user, you can log in with:

username: admin
password: admin

![screenshot](/images/airflow.png)

# Streamlit to visualize the data

Once the DAG has processed te data, you can visualize the stock prices using Streamlit. To run Streamlit:

```
streamlit run src/stream_lit.py
```
This will start the Streamlit app, and you will be able to see an interactive dashboard that visualizes the stock prices. The Streamlit dashboard provides the option to select different columns (e.g., Open, Close, Volume) and view their changes over time.

![screenshot](/images/streamlit_web_1.png)
![screenshot](/images/streamlit_web_2.png)