#!/usr/bin/env python3
import json
from datetime import datetime
import requests
from confluent_kafka import Consumer, KafkaError

KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC = "sensor_data"

ANOMALY_ENDPOINT = "http://localhost:5000/api/anomaly"

consumer_conf = {
    "bootstrap.servers": KAFKA_BOOTSTRAP,
    "group.id": "anomaly-bridge",
    "auto.offset.reset": "latest",
    "enable.auto.commit": True,
}

def forward_anomaly(event: dict):
    try:
        r = requests.post(ANOMALY_ENDPOINT, json=event, timeout=2)
        if r.status_code != 200:
            print(f"[BRIDGE] WS POST failed: {r.status_code} {r.text}")
        else:
            print(f"[BRIDGE] forwarded anomaly for {event.get('sensor_id')}")
    except Exception as e:
        print(f"[BRIDGE] error forwarding anomaly: {e}")

def main():
    c = Consumer(consumer_conf)
    c.subscribe([TOPIC])

    print(f"Kafka anomaly bridge listening on topic '{TOPIC}'…")

    try:
        while True:
            msg = c.poll(1.0)
            if msg is None:
                continue

            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    print(f"[KAFKA] Error: {msg.error()}")
                continue

            try:
                payload = json.loads(msg.value().decode("utf-8"))
            except Exception as e:
                print(f"[KAFKA] Bad JSON: {e}")
                continue

            if payload.get("anomaly") == 1:
                forward_anomaly(payload)

    except KeyboardInterrupt:
        print("Stopping anomaly bridge…")
    finally:
        c.close()

if __name__ == "__main__":
    main()
