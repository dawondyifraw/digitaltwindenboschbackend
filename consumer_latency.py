#!/usr/bin/env python3
import os, json, time, csv, sys
from confluent_kafka import Consumer, KafkaError
import requests

# -------------------
# Config
# -------------------
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
GROUP_ID          = os.getenv("KAFKA_GROUP", "websocket_consumer_group")
TOPICS            = os.getenv("KAFKA_TOPICS", "environment").split(",")

# Telegraf HTTP listener (inputs.http_listener or similar)
TELEGRAF_URL      = os.getenv("TELEGRAF_URL", "http://localhost:8186/telegraf")

# CSV metrics
METRICS_CSV       = os.getenv("METRICS_CSV", "metrics_ingest.csv")

MEASUREMENT       = os.getenv("MEASUREMENT", "env")

def now_ms():
    return int(time.time() * 1000)

def coerce_float(v, default=0.0):
    try:
        return float(v)
    except Exception:
        return float(default)

def to_line_protocol(p):
    # Use producer timestamp as the point time (ns); fall back to now if absent
    prod_ms = int(p.get("ts_producer_ms", now_ms()))
    ts_ns = prod_ms * 1_000_000

    sensor = p.get("sensor_id", "")
    zone   = p.get("zone", "")

    co2   = coerce_float(p.get("co2_ppm"))
    no2   = coerce_float(p.get("no2_ppb"))
    pm25  = coerce_float(p.get("pm25_ugm3"))
    noise = coerce_float(p.get("noise_db"))

    return (
        f"{MEASUREMENT},sensor={sensor},zone={zone} "
        f"co2_ppm={co2},no2_ppb={no2},pm25_ugm3={pm25},noise_db={noise} "
        f"{ts_ns}"
    )

# -------------------
# Kafka consumer
# -------------------
consumer = Consumer({
    'bootstrap.servers': BOOTSTRAP_SERVERS,
    'group.id': GROUP_ID,
    'auto.offset.reset': 'earliest',
})

consumer.subscribe(TOPICS)
print(f"Consuming from topics: {TOPICS}")

# Prepare CSV
csv_exists = os.path.exists(METRICS_CSV)
csv_file = open(METRICS_CSV, "a", newline="")
csv_writer = csv.writer(csv_file)
if not csv_exists:
    csv_writer.writerow([
        "topic","sensor_id","zone",
        "ts_producer_ms","ts_consume_ms","ts_telegraf_ms",
        "prod_to_cons_ms","prod_to_telegraf_ms","cons_to_telegraf_ms"
    ])
    csv_file.flush()

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() != KafkaError._PARTITION_EOF:
                print(f"Kafka error: {msg.error()}", file=sys.stderr)
            continue

        ts_consume = now_ms()

        try:
            payload = json.loads(msg.value().decode("utf-8"))
        except Exception as e:
            print(f"Bad JSON on topic {msg.topic()}: {e}", file=sys.stderr)
            continue

        if "ts_producer_ms" not in payload:
            print(f"[{msg.topic()}] (no ts_producer_ms) {payload}", file=sys.stderr)
            continue

        prod_ms = int(payload["ts_producer_ms"])
        prod_to_cons_ms = ts_consume - prod_ms
        telegraf_ms = None
        prod_to_telegraf_ms = None
        cons_to_telegraf_ms = None

        # Build line protocol and try to hand off to Telegraf
        line = to_line_protocol(payload)
        try:
            t0 = now_ms()
            r = requests.post(
                TELEGRAF_URL,
                data=line.encode("utf-8"),
                headers={"Content-Type": "text/plain"},
                timeout=2.0
            )
            r.raise_for_status()
            telegraf_ms = now_ms()
            prod_to_telegraf_ms = telegraf_ms - prod_ms
            cons_to_telegraf_ms = telegraf_ms - ts_consume
        except Exception as e:
            # Keep running; just log the error
            print(f"[telegraf post failed] {e}", file=sys.stderr)

        # CSV row: use empty string only if truly missing
        csv_writer.writerow([
            msg.topic(),
            payload.get("sensor_id",""),
            payload.get("zone",""),
            prod_ms,
            ts_consume,
            telegraf_ms if telegraf_ms is not None else "",
            prod_to_cons_ms,
            prod_to_telegraf_ms if prod_to_telegraf_ms is not None else "",
            cons_to_telegraf_ms if cons_to_telegraf_ms is not None else "",
        ])
        csv_file.flush()

        # Console pulse
        pulse = f"✓ {msg.topic()} sensor={payload.get('sensor_id','')} prod→cons={prod_to_cons_ms} ms"
        if prod_to_telegraf_ms is not None:
            pulse += f", prod→telegraf={prod_to_telegraf_ms} ms"
        print(pulse)

except KeyboardInterrupt:
    pass
finally:
    consumer.close()
    csv_file.close()
