from confluent_kafka import Producer
import random, time, json, math
from datetime import datetime, timezone

# Kafka Producer Configuration
producer = Producer({
    'bootstrap.servers': 'localhost:9092',
    'client.id': 'sensor-simulator',
    'acks': '1'
})

# Simulation Parameters
SIMULATION_INTERVAL = 5      # seconds
ANOMALY_PROBABILITY = 0.05   # 5%

_rng = random.Random(42)     # reproducible

# Fixed sensors around ’s-Hertogenbosch (stable lat/lon + zone)
SENSORS = [
    {"sensor_id": "IKDB_CONSTRUCTION_01", "zone": "construction", "lat": 51.6900, "lon": 5.3050},
    {"sensor_id": "RESIDENTIAL_01",       "zone": "residential",  "lat": 51.7080, "lon": 5.3150},
    {"sensor_id": "INDUSTRIAL_01",        "zone": "industrial",   "lat": 51.7250, "lon": 5.3600},
]

def now_utc():
    return datetime.now(timezone.utc)

def hour_utc():
    return now_utc().hour

def diurnal(base, amplitude, hour, peak_hour=14):
    # bounded daily sine wave with afternoon peak
    phase = (hour - peak_hour) / 24 * 2 * math.pi
    return base + amplitude * math.sin(phase)

def jitter(scale):
    return _rng.uniform(-scale, scale)

def generate_environmental_data(sensor):
    """CO2 in ppm, NO2 in ppb, PM2.5 in µg/m3"""
    h = hour_utc()
    weather = _rng.choice(['sunny', 'rainy', 'windy', 'cloudy'])
    wx = {'sunny': 1.1, 'cloudy': 1.0, 'rainy': 0.8, 'windy': 0.7}[weather]

    co2_ppm = diurnal(440, 30, h) + jitter(10)
    no2_ppb = diurnal(25, 10, h) + jitter(5)   # use ppb, not ppm
    pm25_ug = diurnal(12, 6, h) + jitter(4)

    if sensor['zone'] == 'construction':
        co2_ppm += 40
        pm25_ug += 8

    co2_ppm *= wx; no2_ppb *= wx; pm25_ug *= wx

    anomaly = 1 if _rng.random() < ANOMALY_PROBABILITY else 0
    if anomaly:
        co2_ppm *= 1.3
        no2_ppb *= 1.2
        pm25_ug *= 1.5

    return {
        'timestamp': now_utc().isoformat(),
        'sensor_id': sensor['sensor_id'],
        'zone': sensor['zone'],
        'latitude': sensor['lat'],
        'longitude': sensor['lon'],
        'weather': weather,
        'co2_ppm': round(max(co2_ppm, 350), 2),
        'no2_ppb': round(max(no2_ppb, 1), 2),
        'pm25_ugm3': round(max(pm25_ug, 1), 2),
        'anomaly': anomaly
    }

def generate_sound_data(sensor):
    """Noise in dB with rush-hour and night patterns"""
    h = hour_utc()
    base = {
        'residential': (45, 65),
        'commercial':  (65, 75),
        'industrial':  (75, 85),
        'construction':(70, 95)
    }
    min_db, max_db = base.get(sensor['zone'], (60, 80))

    time_factor = 1.0
    if 7 <= h <= 9 or 17 <= h <= 19:  # rush hours
        time_factor = 1.4
    if h >= 22 or h <= 6:             # night hours (wraparound)
        if sensor['zone'] == 'residential':
            time_factor = 0.6

    db = _rng.uniform(min_db, max_db) * time_factor

    anomaly = 1 if _rng.random() < ANOMALY_PROBABILITY else 0
    if anomaly:
        db = min(db * 1.5, 110)

    return {
        'timestamp': now_utc().isoformat(),
        'sensor_id': sensor['sensor_id'],
        'zone': sensor['zone'],
        'latitude': sensor['lat'],
        'longitude': sensor['lon'],
        'sound_db': round(db, 2),
        'anomaly': anomaly
    }

def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Delivered to {msg.topic()} [Partition {msg.partition()}]')

def safe_produce(topic, payload):
    data = json.dumps(payload).encode('utf-8')
    while True:
        try:
            producer.produce(topic, data, callback=delivery_report)
            break
        except BufferError:
            producer.poll(0.1)

def produce_data():
    try:
        while True:
            for s in SENSORS:
                safe_produce('environmental_data', generate_environmental_data(s))
                safe_produce('sound_pollution_data', generate_sound_data(s))
            producer.poll(0)
            time.sleep(SIMULATION_INTERVAL)
    except KeyboardInterrupt:
        print("Stopping producer...")
    finally:
        producer.flush()

if __name__ == '__main__':
    print("Starting environmental data simulator (UTC timestamps)…")
    produce_data()
