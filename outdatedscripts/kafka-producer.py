from confluent_kafka import Producer
import random
import time
import json
from datetime import datetime, timezone

# Kafka Producer Configuration
producer = Producer({
    'bootstrap.servers': 'localhost:9092',
    'client.id': 'sensor-simulator',
    'acks': '1'
})

# Simulation Parameters
SIMULATION_INTERVAL = 5  # Seconds between data points
ANOMALY_PROBABILITY = 0.05  # 5% chance of anomaly

def random_coordinate_den_bosch():
    """Generate GPS coordinates within 's-Hertogenbosch bounding box"""
    return (
        round(random.uniform(51.65, 51.75), 6),  # Latitude
        round(random.uniform(5.25, 5.40), 6)     # Longitude
    )

def simulate_trend(base_value, trend_factor):
    """Simulate gradual environmental trend"""
    return base_value + trend_factor * (time.time() / 3600)

def generate_environmental_data():
    """Generate air quality metrics with realistic patterns"""
    gps = random_coordinate_den_bosch()
    weather = random.choice(['sunny', 'rainy', 'windy', 'cloudy'])
    
    # Base values with realistic ranges
    base_values = {
        'co2': simulate_trend(400, 0.5),  # ppm
        'no2': simulate_trend(20, 0.2),   # ppm
        'pm25': simulate_trend(10, 0.1)   # µg/m³
    }
    
    # Weather impact
    weather_modifiers = {
        'rainy': 0.8,
        'windy': 0.7,
        'sunny': 1.1,
        'cloudy': 1.0
    }
    
    # Apply modifiers and anomalies
    return {
        'timestamp': datetime.now(timezone.utc).isoformat(),
        'latitude': gps[0],
        'longitude': gps[1],
        'weather': weather,
        'co2': round(base_values['co2'] * weather_modifiers[weather], 2),
        'no2': round(base_values['no2'] * weather_modifiers[weather], 2),
        'pm25': round(base_values['pm25'] * weather_modifiers[weather], 2),
        'anomaly': int(random.random() < ANOMALY_PROBABILITY)
    }

def generate_sound_data():
    """Generate noise pollution data with temporal patterns"""
    gps = random_coordinate_den_bosch()
    current_hour = datetime.now().hour
    
    # Location-based base levels
    locations = {
        'residential': (45, 65),
        'commercial': (65, 75),
        'industrial': (75, 85)
    }
    
    location_type = random.choice(list(locations.keys()))
    min_db, max_db = locations[location_type]
    
    # Temporal adjustments
    time_modifier = 1.0
    if 22 <= current_hour <= 6:  # Night hours
        if location_type == 'residential':
            time_modifier = 0.6
    elif 7 <= current_hour <= 9 or 17 <= current_hour <= 19:  # Rush hours
        if location_type in ['commercial', 'industrial']:
            time_modifier = 1.4
    
    return {
        'timestamp': datetime.now(timezone.utc).isoformat(),
        'latitude': gps[0],
        'longitude': gps[1],
        'location_type': location_type,
        'sound_level': round(random.uniform(min_db, max_db) * time_modifier, 2)
    }

def delivery_report(err, msg):
    """Handle message delivery reports"""
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Delivered to {msg.topic()} [Partition {msg.partition()}]')

def produce_data():
    """Main production loop"""
    try:
        while True:
            # Environmental data
            env_data = generate_environmental_data()
            producer.produce(
                'environmental_data',
                json.dumps(env_data),
                callback=delivery_report
            )
            
            # Sound data
            sound_data = generate_sound_data()
            producer.produce(
                'sound_pollution_data',
                json.dumps(sound_data),
                callback=delivery_report
            )
            
            producer.poll(0)
            time.sleep(SIMULATION_INTERVAL)
            
    except KeyboardInterrupt:
        print("Stopping producer...")
    finally:
        producer.flush()

if __name__ == '__main__':
    print("Starting environmental data simulator...")
    produce_data()