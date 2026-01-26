#!/usr/bin/env python3
# 50-sensor correlated simulator + anomaly detection + WebSocket ingest
# Feeds: ws://localhost:6790 (your anomaly distributor)

import asyncio
import json
import math
import random
import websockets
from datetime import datetime, timezone

# -----------------------------------------
# CONFIG
# -----------------------------------------
NUM_SENSORS = 50
ANOMALY_PROBABILITY = 0.05
random.seed(42)

WS_INGEST_URL = "ws://localhost:6790"  # your ingest server from earlier
SEND_INTERVAL = 1.0  # seconds


# -----------------------------------------
# ANOMALY DETECTOR (your 7-rule version)
# -----------------------------------------
def detect_anomaly(r):
    co2  = r.get("co2_ppm", 0)
    no2  = r.get("no2_ppb", 0)
    pm25 = r.get("pm25_ugm3", 0)
    noise = r.get("noise_db", 0)

    # Single-parameter spikes
    if co2 > 900:
        return "co2_spike"
    if noise > 90:
        return "noise_spike"
    if no2 > 60:
        return "no2_spike"
    if pm25 > 40:
        return "pm25_spike"

    # Correlated spikes
    if co2 > 800 and noise > 80:
        return "correlated_co2_noise"
    if noise > 85 and pm25 > 35:
        return "correlated_noise_pm25"

    # Rare triple event
    if co2 > 850 and no2 > 50 and pm25 > 35:
        return "triple_spike"

    return None


# -----------------------------------------
# SENSOR SETUP
# -----------------------------------------
# Spread sensors around Den Bosch in a real geographic cluster
def random_coord(lat0=51.695, lon0=5.31):
    return (
        round(lat0 + random.uniform(-0.015, 0.015), 6),
        round(lon0 + random.uniform(-0.020, 0.020), 6)
    )

def random_zone():
    return random.choice(["residential", "construction", "industrial", "commercial"])

SENSORS = []
for i in range(NUM_SENSORS):
    lat, lon = random_coord()
    zone = random_zone()
    SENSORS.append({
        "sensor_id": f"S-{i+1:03d}",
        "zone": zone,
        "lat": lat,
        "lon": lon
    })


# -----------------------------------------
# ENVIRONMENTAL MODEL (correlated)
# -----------------------------------------
def diurnal(base, amplitude, hour, peak=14):
    phi = (hour - peak) / 24 * 2 * math.pi
    return base + amplitude * math.sin(phi)

def sample_sensor(s):
    t = datetime.now(timezone.utc)
    hour = t.hour

    # Primary CO2 sinusoidal profile
    co2 = diurnal(450, 50, hour) + random.uniform(-10, 10)

    # Correlate noise: when CO2 increases (traffic/machinery), noise increases too
    noise = (
        diurnal(50, 15, hour)
        + (co2 - 450) * 0.12
        + random.uniform(-4, 4)
    )

    # PM2.5 correlated with noise
    pm25 = (
        diurnal(12, 6, hour)
        + (noise - 50) * 0.25
        + random.uniform(-2, 2)
    )

    # NO2 somewhat correlated to traffic CO2 as well
    no2 = (
        diurnal(25, 10, hour)
        + (co2 - 450) * 0.05
        + random.uniform(-3, 3)
    )

    # Weather factors
    weather = random.choice(["sunny", "cloudy", "rainy", "windy"])
    wx = {"sunny": 1.1, "cloudy": 1.0, "rainy": 0.8, "windy": 0.7}[weather]

    co2 *= wx
    no2 *= wx
    pm25 *= wx

    # Random anomaly injection
    anomaly_flag = 0
    if random.random() < ANOMALY_PROBABILITY:
        anomaly_flag = 1
        co2 *= 1.4
        noise *= 1.4
        pm25 *= 1.6
        no2 *= 1.3

    reading = {
        "timestamp": t.isoformat(),
        "sensor_id": s["sensor_id"],
        "zone": s["zone"],
        "latitude": s["lat"],
        "longitude": s["lon"],
        "weather": weather,
        "co2_ppm": round(co2, 2),
        "no2_ppb": round(no2, 2),
        "pm25_ugm3": round(pm25, 2),
        "noise_db": round(noise, 2),
        "anomaly": anomaly_flag
    }

    # Apply analytical anomaly detector
    atype = detect_anomaly(reading)
    reading["anomaly_type"] = atype

    return reading


# -----------------------------------------
# MAIN ASYNC LOOP
# -----------------------------------------
async def main():
    print(f"Starting 50-sensor simulator -> {WS_INGEST_URL}")
    print("Press Ctrl+C to stop.")

    async with websockets.connect(WS_INGEST_URL) as ws:
        while True:
            batch = []
            for s in SENSORS:
                r = sample_sensor(s)
                if r["anomaly_type"]:
                    # Only send anomalies to the ingest WS
                    await ws.send(json.dumps(r))
                batch.append(r)

            # optional: print some signal
            print(f"Generated {len(batch)} readings | anomalies: {sum(1 for x in batch if x['anomaly_type'])}")

            await asyncio.sleep(SEND_INTERVAL)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nStopped simulator.")
