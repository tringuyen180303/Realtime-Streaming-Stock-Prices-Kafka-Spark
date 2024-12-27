import json
from kafka import KafkaConsumer

KAFKA_BROKER = 'localhost:9092'
TOPICS = ['stock_prices']

consumer = KafkaConsumer(*TOPICS,
    bootstrap_servers = KAFKA_BROKER,
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='my_group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')
    ))
    

print(f"Listening to topics: {TOPICS}")

for message in consumer:
    topic = message.topic
    data = message.value
    print(f"\nReceived message from {topic}:")
    print(json.dumps(data, indent=2))