#!/usr/bin/env python3

from confluent_kafka import Consumer

from confluent_kafka import Consumer

# Kafka consumer setup
consumer = Consumer({
    'bootstrap.servers': 'localhost:9092',  # Kafka broker address
    'group.id': 'websocket_consumer_group',  # Consumer group ID
    'auto.offset.reset': 'earliest'          # Start consuming from the earliest offset
})

# Subscribe to the desired Kafka topics
consumer.subscribe(['environmental_data', 'sound_pollution_data'])

# Poll Kafka for messages
while True:
    msg = consumer.poll(1.0)  # Poll Kafka every second
    if msg is None:
        continue
    if msg.error():
        print(f"Kafka error: {msg.error()}")
        continue

    # Decode and process the message
    data = msg.value().decode('utf-8')
    print(f"Received data from Kafka topic {msg.topic()}: {data}")