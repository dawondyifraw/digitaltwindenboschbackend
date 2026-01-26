#!/usr/bin/env python3
# Proper SENSOR SIMULATOR → Kafka
# No InfluxDB logic. No CSV. No batching.
# This behaves like a real device.

from confluent_kafka import Producer
import time, random, math, json
from datetime import datetime, timezone

# ----------------------------
# Kafka Producer Configuration
# ----------------------------
producer = Producer({
    'bootstrap.servers': 'localhost:9092',
    'client.id': 'sensor-simulator',
    'acks': '1'
})

# ----------------------------
# Simulation settings
# ----------------------------
INTERVAL_SEC = 1
ANOMALY_PROB = 0.05
random.seed(42)

# ----------------------------
# Sensor definitions
# ----------------------------
SENSORS = [
    {"sensor_id": "S-C1", "zone": "construction", "lat": 51.6903, "lon": 5.3030},
    {"sensor_id": "S-R1", "zone": "residential",  "lat": 51.6921, "lon": 5.3075},
    {"sensor_id": "S-I1", "zone": "industrial",   "lat": 51.6867, "lon": 5.2982},
]

def now_iso():
    return datetime.now(timezone.utc).isoformat()

def diurnal(base, amplitude, hour, peak_hour=14):
    phi = (hour - peak_hour) / 24 * 2 * math.pi
    return base + amplitude * math.sin(phi)

def noise_base(zone):
    if zone == "construction":
        return 65
    if zone == "residential":
        return 45
    return 55  # industrial/commercial baseline

def generate_reading(sensor):
    hour = datetime.now().hour

    # Environmental signals
    co2  = diurnal(500, 200, hour) + random.gauss(0, 15)
    no2  = diurnal(20, 10, hour)  + random.gauss(0, 3)
    pm25 = diurnal(8, 6, hour)    + random.gauss(0, 2)

    # Noise
    base = noise_base(sensor["zone"])
    noise = base + 8 * math.sin(3 * hour) + random.gauss(0, 4)

    # Anomaly?
    anomaly = 1 if random.random() < ANOMALY_PROB else 0
    if anomaly:
        co2  *= 1.8
        no2  *= 2.0
        pm25 *= 2.5
        noise *= 1.4

    # Clamp & round
    return {
        "timestamp": now_iso(),
        "sensor_id": sensor["sensor_id"],
        "zone": sensor["zone"],
        "latitude": sensor["lat"],
        "longitude": sensor["lon"],
        "co2_ppm": round(max(co2, 350), 2),
        "no2_ppb": round(max(no2, 1), 2),
        "pm25_ugm3": round(max(pm25, 1), 2),
        "noise_db": round(max(noise, 30), 1),
        "anomaly": anomaly
    }

def delivery_report(err, msg):
    if err:
        print("Delivery failed:", err)
    else:
        print(f"Delivered to {msg.topic()} | {msg.partition()}")

def sensor_loop():
    print("Starting SENSOR SIMULATOR → Kafka")
    print("Sensors:", [s["sensor_id"] for s in SENSORS])
    
    try:
        while True:
            for s in SENSORS:
                reading = generate_reading(s)
                producer.produce(
                    "environmental_data",
                    json.dumps(reading),
                    callback=delivery_report
                )
            producer.poll(0)
            time.sleep(INTERVAL_SEC)

    except KeyboardInterrupt:
        print("Stopping...")
    finally:
        producer.flush()

if __name__ == "__main__":
    sensor_loop()
