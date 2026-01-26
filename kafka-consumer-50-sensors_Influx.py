#!/usr/bin/env python3
import os
import json
import logging
from confluent_kafka import Consumer
from influxdb_client import InfluxDBClient, Point, WriteOptions

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
log = logging.getLogger("kafka-influx-consumer")

# =========================
# CONFIG
# =========================
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")  # or "kafka:9092" in Docker
KAFKA_GROUP     = os.getenv("KAFKA_GROUP", "sensor_data_ingest")
KAFKA_TOPIC     = os.getenv("KAFKA_TOPIC", "sensor_data")

INFLUX_URL      = os.getenv("INFLUX_URL", "http://localhost:8086")  # or "http://influxdb:8086"
INFLUX_TOKEN    = os.getenv("INFLUX_TOKEN", "vQZH5VT3VRUrDBdX4oyDO5BV7kWN4NvRvUJUSvkOGEz-cL3huzmpkBo5ywMVBioDXNQ0UfHc3afinUpxFnLmA==")
INFLUX_ORG      = os.getenv("INFLUX_ORG", "DenBosch")
INFLUX_BUCKET   = os.getenv("INFLUX_BUCKET", "sensors_db")
MEASUREMENT     = "environment"

# =========================
# KAFKA CONSUMER
# =========================
consumer = Consumer({
    "bootstrap.servers": KAFKA_BOOTSTRAP,
    "group.id": KAFKA_GROUP,
    "auto.offset.reset": "latest",
})

consumer.subscribe([KAFKA_TOPIC])

# =========================
# INFLUX CLIENT
# =========================
client = InfluxDBClient(
    url=INFLUX_URL,
    token=INFLUX_TOKEN,
    org=INFLUX_ORG,
)

write_api = client.write_api(
    write_options=WriteOptions(batch_size=1, flush_interval=1_000)
)

log.info(f"📥 Kafka → Influx consumer running on topic={KAFKA_TOPIC}, bucket={INFLUX_BUCKET}")


def to_point(data: dict) -> Point:
    """
    Map JSON from sensor_data topic to Influx Point.
    Expected payload from producer:
      {
        "timestamp": "...",
        "sensor_id": "...",
        "zone": "...",
        "latitude": 51.x,
        "longitude": 5.x,
        "co2_ppm": ...,
        "no2_ppb": ...,
        "pm25_ugm3": ...,
        "noise_db": ...,
        "anomaly": 0/1
      }
    """
    required = [
        "timestamp", "sensor_id", "zone",
        "latitude", "longitude",
        "co2_ppm", "no2_ppb", "pm25_ugm3", "noise_db"
    ]
    missing = [k for k in required if k not in data]
    if missing:
        raise ValueError(f"Missing keys in payload: {missing}")

    p = (
        Point(MEASUREMENT)
        .tag("sensor_id", str(data["sensor_id"]))
        .tag("zone", str(data["zone"]))
        .field("co2_ppm", float(data["co2_ppm"]))
        .field("no2_ppb", float(data["no2_ppb"]))
        .field("pm25_ugm3", float(data["pm25_ugm3"]))
        .field("noise_db", float(data["noise_db"]))
        .field("latitude", float(data["latitude"]))
        .field("longitude", float(data["longitude"]))
        .field("anomaly", int(data.get("anomaly", 0)))
        .time(data["timestamp"])  # ISO8601 from producer
    )
    return p


try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue

        if msg.error():
            log.error(f"Kafka error: {msg.error()}")
            continue

        try:
            raw = msg.value()
            data = json.loads(raw)
            point = to_point(data)

            write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=point)

            log.info(
                f"✓ wrote sensor={data['sensor_id']} "
                f"zone={data['zone']} "
                f"co2={data['co2_ppm']} noise={data['noise_db']} "
                f"anomaly={data.get('anomaly', 0)}"
            )

        except Exception as e:
            log.error(f"Parse/Influx error for message {msg.value()}: {e}")

except KeyboardInterrupt:
    log.info("Stopping consumer…")
finally:
    consumer.close()
    client.close()
