#!/usr/bin/env python3
"""
Kafka Producer Simulator
AUTHOR: Daniel Wondyifraw DataTwinLabs.nl

Simulates sensor data and produces messages to Kafka topics for testing.
"""

import random, math, time, json
from datetime import datetime, timezone
from confluent_kafka import Producer

# ============================================================
# KAFKA CONFIG
# ============================================================
producer = Producer({
    "bootstrap.servers": "localhost:9092",   # if script runs on HOST
    # "bootstrap.servers": "kafka:9092",     # if script runs in Docker next to kafka
    "client.id": "sensor-simulator",
    "acks": "1"
})

TOPIC = "sensor_data"

# ============================================================
# 50 REALISTIC SENSORS ACROSS DEN BOSCH
# ============================================================
SENSORS = [
    # Construction zone (12)
    {"sensor_id": "C-01", "zone": "construction", "lat": 51.6901, "lon": 5.3034},
    {"sensor_id": "C-02", "zone": "construction", "lat": 51.6915, "lon": 5.3081},
    {"sensor_id": "C-03", "zone": "construction", "lat": 51.6932, "lon": 5.3103},
    {"sensor_id": "C-04", "zone": "construction", "lat": 51.6948, "lon": 5.3157},
    {"sensor_id": "C-05", "zone": "construction", "lat": 51.6960, "lon": 5.3199},
    {"sensor_id": "C-06", "zone": "construction", "lat": 51.6978, "lon": 5.3240},
    {"sensor_id": "C-07", "zone": "construction", "lat": 51.6991, "lon": 5.3278},
    {"sensor_id": "C-08", "zone": "construction", "lat": 51.7005, "lon": 5.3321},
    {"sensor_id": "C-09", "zone": "construction", "lat": 51.7021, "lon": 5.3364},
    {"sensor_id": "C-10", "zone": "construction", "lat": 51.7035, "lon": 5.3402},
    {"sensor_id": "C-11", "zone": "construction", "lat": 51.7050, "lon": 5.3445},
    {"sensor_id": "C-12", "zone": "construction", "lat": 51.7064, "lon": 5.3488},

    # Residential (18)
    {"sensor_id": "R-01", "zone": "residential", "lat": 51.6742, "lon": 5.2764},
    {"sensor_id": "R-02", "zone": "residential", "lat": 51.6761, "lon": 5.2807},
    {"sensor_id": "R-03", "zone": "residential", "lat": 51.6784, "lon": 5.2850},
    {"sensor_id": "R-04", "zone": "residential", "lat": 51.6803, "lon": 5.2892},
    {"sensor_id": "R-05", "zone": "residential", "lat": 51.6821, "lon": 5.2928},
    {"sensor_id": "R-06", "zone": "residential", "lat": 51.6840, "lon": 5.2975},
    {"sensor_id": "R-07", "zone": "residential", "lat": 51.6861, "lon": 5.3009},
    {"sensor_id": "R-08", "zone": "residential", "lat": 51.6884, "lon": 5.3048},
    {"sensor_id": "R-09", "zone": "residential", "lat": 51.6905, "lon": 5.3072},
    {"sensor_id": "R-10", "zone": "residential", "lat": 51.6924, "lon": 5.3106},
    {"sensor_id": "R-11", "zone": "residential", "lat": 51.6942, "lon": 5.3149},
    {"sensor_id": "R-12", "zone": "residential", "lat": 51.6961, "lon": 5.3183},
    {"sensor_id": "R-13", "zone": "residential", "lat": 51.6983, "lon": 5.3217},
    {"sensor_id": "R-14", "zone": "residential", "lat": 51.7004, "lon": 5.3250},
    {"sensor_id": "R-15", "zone": "residential", "lat": 51.7025, "lon": 5.3282},
    {"sensor_id": "R-16", "zone": "residential", "lat": 51.7043, "lon": 5.3316},
    {"sensor_id": "R-17", "zone": "residential", "lat": 51.7064, "lon": 5.3351},
    {"sensor_id": "R-18", "zone": "residential", "lat": 51.7081, "lon": 5.3383},

    # Industrial (10)
    {"sensor_id": "I-01", "zone": "industrial", "lat": 51.7321, "lon": 5.3550},
    {"sensor_id": "I-02", "zone": "industrial", "lat": 51.7335, "lon": 5.3591},
    {"sensor_id": "I-03", "zone": "industrial", "lat": 51.7304, "lon": 5.3512},
    {"sensor_id": "I-04", "zone": "industrial", "lat": 51.7282, "lon": 5.3475},
    {"sensor_id": "I-05", "zone": "industrial", "lat": 51.7260, "lon": 5.3436},
    {"sensor_id": "I-06", "zone": "industrial", "lat": 51.7247, "lon": 5.3404},
    {"sensor_id": "I-07", "zone": "industrial", "lat": 51.7221, "lon": 5.3361},
    {"sensor_id": "I-08", "zone": "industrial", "lat": 51.7202, "lon": 5.3330},
    {"sensor_id": "I-09", "zone": "industrial", "lat": 51.7185, "lon": 5.3295},
    {"sensor_id": "I-10", "zone": "industrial", "lat": 51.7163, "lon": 5.3261},

    # Commercial (10)
    {"sensor_id": "M-01", "zone": "commercial", "lat": 51.6831, "lon": 5.3509},
    {"sensor_id": "M-02", "zone": "commercial", "lat": 51.6854, "lon": 5.3547},
    {"sensor_id": "M-03", "zone": "commercial", "lat": 51.6872, "lon": 5.3583},
    {"sensor_id": "M-04", "zone": "commercial", "lat": 51.6893, "lon": 5.3621},
    {"sensor_id": "M-05", "zone": "commercial", "lat": 51.6910, "lon": 5.3664},
    {"sensor_id": "M-06", "zone": "commercial", "lat": 51.6931, "lon": 5.3700},
    {"sensor_id": "M-07", "zone": "commercial", "lat": 51.6950, "lon": 5.3733},
    {"sensor_id": "M-08", "zone": "commercial", "lat": 51.6974, "lon": 5.3771},
    {"sensor_id": "M-09", "zone": "commercial", "lat": 51.6992, "lon": 5.3805},
    {"sensor_id": "M-10", "zone": "commercial", "lat": 51.7011, "lon": 5.3837},
]

# ============================================================
# SIMULATION PARAMETERS
# ============================================================
EMIT_INTERVAL = 3.0
ANOMALY_PROB = 0.05
rng = random.Random(42)

def now_iso():
    return datetime.now(timezone.utc).isoformat()

def hour():
    return datetime.now(timezone.utc).hour

# ============================================================
# CORRELATED SENSOR MODEL
# ============================================================
def correlated_activity(zone):
    h = hour()
    base = math.sin((h / 24) * 2 * math.pi)
    return {
        "construction": base * 1.4 + 0.6,
        "industrial":   base * 1.2 + 0.4,
        "commercial":   base * 1.0 + 0.3,
        "residential":  base * 0.8 + 0.2,
    }.get(zone, base)

def generate_sensor_data(s):
    activity = correlated_activity(s["zone"])
    j = lambda sc: rng.uniform(-sc, sc)

    co2 = 420 + 90 * activity + j(10)
    no2 = 18 + 14 * activity + j(4)
    pm25 = 9 + 7 * activity + j(3)
    noise = 48 + 22 * activity + j(6)

    if s["zone"] == "construction":
        noise += 18
        pm25 += 6
    if s["zone"] == "industrial":
        no2 += 8
    if s["zone"] == "commercial":
        noise += 5

    anomaly = 0
    if rng.random() < ANOMALY_PROB:
        anomaly = 1
        spike = rng.uniform(1.3, 1.7)
        co2 *= spike
        no2 *= spike
        pm25 *= spike
        noise *= spike

    return {
        "timestamp": now_iso(),
        "sensor_id": s["sensor_id"],
        "zone": s["zone"],
        "latitude": s["lat"],
        "longitude": s["lon"],
        "co2_ppm": round(max(co2, 300), 2),
        "no2_ppb": round(max(no2, 1), 2),
        "pm25_ugm3": round(max(pm25, 1), 2),
        "noise_db": round(max(noise, 28), 2),
        "anomaly": anomaly
    }

# ============================================================
# DELIVERY REPORT
# ============================================================
def delivery_report(err, msg):
    if err is not None:
        print(f"❌ Delivery failed: {err}")
    else:
        print(f"✅ Delivered to {msg.topic()} [{msg.partition()}] offset={msg.offset()}")

# ============================================================
# MAIN LOOP
# ============================================================
if __name__ == "__main__":
    print("Starting 50-sensor correlated simulator → Kafka (sensor_data)…")

    try:
        while True:
            for s in SENSORS:
                reading = generate_sensor_data(s)
                producer.produce(
                    TOPIC,
                    json.dumps(reading).encode("utf-8"),
                    callback=delivery_report
                )
            # Let librdkafka handle delivery callbacks
            producer.poll(0)
            time.sleep(EMIT_INTERVAL)
    except KeyboardInterrupt:
        print("Stopping producer…")
    finally:
        print("Flushing…")
        producer.flush()