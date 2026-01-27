# producer_sim_influx_direct.py
import time, random, math, requests
from datetime import datetime, timezone

# InfluxDB 2.x Direct API Configuration
INFLUXDB_URL = 'http://localhost:8086/api/v2/write'
DOCKER_INFLUXDB_INIT_OR = "DenBosch"
DOCKER_INFLUXDB_INIT_BUCKE ="sensors_db"
DOCKER_INFLUXDB_INIT_ADMIN_TOKEN = "vQZH5VT3VRUrDBdX4oyDO5BV7kWN4NvRvUJUSvkOGEz-cL3huzmpkBo5ywMVBioDXNQ0UfHc3afinUpxFnLmA=="

HEADERS = {
    'Authorization': f'Token {DOCKER_INFLUXDB_INIT_ADMIN_TOKEN}',
    'Content-Type': 'text/plain'
}
EMIT_INTERVAL_SEC = 1.0

random.seed(42)

def now_ns():
    """Get current time in nanoseconds for InfluxDB precision"""
    return int(time.time() * 1_000_000_000)

class Sensor:
    def __init__(self, sid, zone, lat, lon):
        self.id = sid
        self.zone = zone
        self.lat = lat
        self.lon = lon

    def sample(self):
        t = time.time()
        day = 86400.0
        phi = 2 * math.pi * ((t % day) / day)

        # Very simple diurnal patterns + noise
        co2 = 500 + 200 * math.sin(phi) + random.gauss(0, 15)
        no2 = 20 + 10 * math.sin(2 * phi) + random.gauss(0, 3)
        pm25 = 8 + 6 * math.sin(1.5 * phi) + random.gauss(0, 2)

        base_noise = 45 if self.zone == "residential" else 55
        if self.zone == "construction":
            base_noise = 65
        noise = base_noise + 8 * math.sin(3 * phi) + random.gauss(0, 4)

        return {
            "co2_ppm": round(max(co2, 350), 2),
            "no2_ppb": round(max(no2, 1), 2),
            "pm25_ugm3": round(max(pm25, 1), 2),
            "noise_db": round(max(noise, 30), 1),
        }

def format_influx_line_protocol(sensor, data):
    """Convert sensor data to InfluxDB Line Protocol"""
    timestamp = now_ns()
    
    # Escape spaces and commas in tag values
    sensor_id = sensor.id.replace(' ', '\\ ').replace(',', '\\,')
    zone = sensor.zone.replace(' ', '\\ ').replace(',', '\\,')
    
    # Format: measurement,tag1=value1,tag2=value2 field1=value1,field2=value2 timestamp
    line = f"environment,sensor_id={sensor_id},zone={zone} "
    line += f"co2_ppm={data['co2_ppm']},no2_ppb={data['no2_ppb']},pm25_ugm3={data['pm25_ugm3']},noise_db={data['noise_db']},latitude={sensor.lat},longitude={sensor.lon} "
    line += str(timestamp)
    
    return line

def safe_send_to_influxdb(data_lines):
    """Send data to InfluxDB with error handling"""
    try:
        # Join multiple lines with newline separator for batch insert
        payload = '\n'.join(data_lines)
        
        params = {
            'org': DOCKER_INFLUXDB_INIT_OR,
            'bucket': DOCKER_INFLUXDB_INIT_BUCKE,
            'precision': 'ns'
        }
        
        response = requests.post(INFLUXDB_URL, params=params, data=payload, headers=HEADERS, timeout=5)
        
        if response.status_code == 204:
            print(f"✓ Successfully sent {len(data_lines)} measurements to InfluxDB")
            return True
        else:
            print(f"✗ InfluxDB error: {response.status_code} - {response.text}")
            return False
            
    except requests.exceptions.ConnectionError:
        print("✗ Cannot connect to InfluxDB. Is it running on localhost:8086?")
        return False
    except requests.exceptions.Timeout:
        print("✗ InfluxDB request timeout")
        return False
    except Exception as e:
        print(f"✗ Unexpected error: {e}")
        return False

# Define sensors
sensors = [
    Sensor("S-C1", "construction", 51.6903, 5.3030),
    Sensor("S-R1", "residential", 51.6921, 5.3075),
    Sensor("S-I1", "industrial", 51.6867, 5.2982),
]

print(f"Starting sensor simulator -> InfluxDB API (No Telegraf)")
print(f"Sensors: {[s.id for s in sensors]}")
print(f"Interval: {EMIT_INTERVAL_SEC}s")
print("Press Ctrl+C to stop...")

# Test connection first
print("Testing InfluxDB connection...")
test_line = format_influx_line_protocol(sensors[0], sensors[0].sample())
if safe_send_to_influxdb([test_line]):
    print("Connection successful! Starting data stream...")
else:
    print("Connection failed. Please check:")
    print(f"  - InfluxDB running on localhost:8086")
    print(f"  - Organization: {DOCKER_INFLUXDB_INIT_OR}")
    print(f"  - Bucket: {DOCKER_INFLUXDB_INIT_BUCKE}")
    print(f"  - Token: {DOCKER_INFLUXDB_INIT_ADMIN_TOKEN[:10]}...")
    exit(1)

while True:
    try:
        batch_lines = []
        
        for sensor in sensors:
            data = sensor.sample()
            line = format_influx_line_protocol(sensor, data)
            batch_lines.append(line)
        
        # Send all sensor data in one batch request
        success = safe_send_to_influxdb(batch_lines)
        
        if success:
            # Quick console output
            current_time = time.strftime('%H:%M:%S')
            sample_data = sensors[0].sample()
            print(f"[{current_time}] Sample: CO₂={sample_data['co2_ppm']}ppm, Noise={sample_data['noise_db']}dB")
        
        time.sleep(EMIT_INTERVAL_SEC)
        
    except KeyboardInterrupt:
        print("\nStopping sensor simulator...")
        break
    except Exception as e:
        print(f"Unexpected error in main loop: {e}")
        time.sleep(EMIT_INTERVAL_SEC)