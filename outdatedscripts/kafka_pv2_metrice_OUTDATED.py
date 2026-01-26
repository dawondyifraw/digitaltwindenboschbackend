#!/usr/bin/env python3
# Direct InfluxDB writer + metrics CSV logger WITH ANOMALY DETECTION
# - No Telegraf/Kafka in this path
# - Batch writes to InfluxDB v2 API
# - Logs per-sensor rows with a run_id and http_ms latency
# - Includes 5% anomaly probability

import time, random, math, requests, uuid, csv, os
from datetime import datetime, timezone

# ----------------------------
# InfluxDB 2.x Direct API
# ----------------------------
INFLUXDB_URL = 'http://localhost:8086/api/v2/write'
ORG          = "DenBosch"
BUCKET       = "sensors_db"
TOKEN        = "vQZH5VT3VRUrDBdX4oyDO5BV7kWN4NvRvUJUSvkOGEz-cL3huzmpkBo5ywMVBioDXNQ0UfHc3afinUpxFnLmA=="

HEADERS = {
    'Authorization': f'Token {TOKEN}',
    'Content-Type': 'text/plain',
}

# ----------------------------
# Emission settings
# ----------------------------
EMIT_INTERVAL_SEC = 1.0
ANOMALY_PROBABILITY = 0.05  # 5% chance of anomaly
random.seed(42)

# ----------------------------
# Metrics CSV
# ----------------------------
CSV_FILE = "metrics_ingest.csv"
RUN_ID   = str(uuid.uuid4())

def ensure_csv_header(path):
    exists = os.path.exists(path)
    f = open(path, "a", newline="")
    w = csv.writer(f)
    if not exists:
        w.writerow([
            "run_id","batch_id","sensor_id","zone","latitude","longitude",
            "ts_ns","status","http_ms","anomaly",
            "co2_ppm","no2_ppb","pm25_ugm3","noise_db"
        ])
        f.flush()
    return f, w

csv_fh, csv_writer = ensure_csv_header(CSV_FILE)

# ----------------------------
# Helpers
# ----------------------------
def now_ns():
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

        # simple diurnal-ish profiles + noise
        co2  = 500 + 200 * math.sin(phi) + random.gauss(0, 15)
        no2  =  20 +  10 * math.sin(2 * phi) + random.gauss(0, 3)
        pm25 =   8 +   6 * math.sin(1.5 * phi) + random.gauss(0, 2)

        base_noise = 45 if self.zone == "residential" else 55
        if self.zone == "construction":
            base_noise = 65
        noise = base_noise + 8 * math.sin(3 * phi) + random.gauss(0, 4)

        # Apply anomaly with 5% probability
        anomaly = 0
        if random.random() < ANOMALY_PROBABILITY:
            anomaly = 1
            # Spike the values for anomalies
            co2 *= 1.8    # 80% increase
            no2 *= 2.0    # 100% increase  
            pm25 *= 2.5   # 150% increase
            noise *= 1.4  # 40% increase

        return {
            "co2_ppm": round(max(co2, 350), 2),
            "no2_ppb": round(max(no2, 1), 2),
            "pm25_ugm3": round(max(pm25, 1), 2),
            "noise_db": round(max(noise, 30), 1),
            "anomaly": anomaly
        }

def lp_escape(v: str) -> str:
    return str(v).replace(' ', r'\ ').replace(',', r'\,')

def to_line_protocol(sensor: Sensor, fields: dict, ts_ns: int) -> str:
    tags = f"sensor_id={lp_escape(sensor.id)},zone={lp_escape(sensor.zone)}"
    # include lat/lon as fields (numeric) and anomaly as field
    return (
        f"environment,{tags} "
        f"co2_ppm={fields['co2_ppm']},"
        f"no2_ppb={fields['no2_ppb']},"
        f"pm25_ugm3={fields['pm25_ugm3']},"
        f"noise_db={fields['noise_db']},"
        f"anomaly={fields['anomaly']},"
        f"latitude={sensor.lat},longitude={sensor.lon} "
        f"{ts_ns}"
    )

def send_batch(lines):
    params = {'org': ORG, 'bucket': BUCKET, 'precision': 'ns'}
    t0 = time.time()
    r = requests.post(INFLUXDB_URL, params=params, data="\n".join(lines),
                      headers=HEADERS, timeout=5)
    t1 = time.time()
    http_ms = int((t1 - t0) * 1000)
    return r.status_code, r.text, http_ms

# ----------------------------
# Sensors
# ----------------------------
sensors = [
    Sensor("S-C1", "construction", 51.6903, 5.3030),
    Sensor("S-R1", "residential",  51.6921, 5.3075),
    Sensor("S-I1", "industrial",   51.6867, 5.2982),
]

print("Starting sensor simulator -> InfluxDB API (no Telegraf)")
print(f"Run ID: {RUN_ID}")
print(f"Sensors: {[s.id for s in sensors]}")
print(f"Interval: {EMIT_INTERVAL_SEC}s")
print(f"Anomaly probability: {ANOMALY_PROBABILITY*100}%")
print("Testing InfluxDB connection...")

# Quick connectivity check
test_ts = now_ns()
test_sample = sensors[0].sample()
test_line = to_line_protocol(sensors[0], test_sample, test_ts)
code, body, ms = send_batch([test_line])
if code == 204:
    print("✓ InfluxDB connection OK. Starting stream…")
    print(f"  Test sample - CO₂: {test_sample['co2_ppm']}ppm, Anomaly: {test_sample['anomaly']}")
else:
    print(f"✗ InfluxDB write failed ({code}): {body}")
    print("Check: influxd up on :8086, ORG/BUCKET/TOKEN correct.")
    csv_fh.close()
    raise SystemExit(1)

# ----------------------------
# Main loop
# ----------------------------
batch_counter = 0
anomaly_count = 0
try:
    while True:
        batch_counter += 1
        batch_id = f"b{batch_counter:06d}"

        # sample & build LP
        lines = []
        stamped = []  # hold (sensor, fields, ts_ns) for logging
        for s in sensors:
            fields = s.sample()
            ts_ns = now_ns()
            lines.append(to_line_protocol(s, fields, ts_ns))
            stamped.append((s, fields, ts_ns))
            if fields['anomaly'] == 1:
                anomaly_count += 1

        # send
        code, body, http_ms = send_batch(lines)
        ok = (code == 204)

        # log per-sensor rows to CSV
        for s, fields, ts_ns in stamped:
            csv_writer.writerow([
                RUN_ID, batch_id, s.id, s.zone, s.lat, s.lon,
                ts_ns, ("OK" if ok else f"ERR{code}"), http_ms, fields["anomaly"],
                fields["co2_ppm"], fields["no2_ppb"], fields["pm25_ugm3"], fields["noise_db"]
            ])
        csv_fh.flush()

        # console pulse with anomaly info
        current_anomalies = sum(1 for s, fields, ts_ns in stamped if fields['anomaly'] == 1)
        anomaly_indicator = " 🚨" if current_anomalies > 0 else ""
        
        if ok:
            print(f"✓ wrote {len(lines)} pts | http_ms={http_ms} | anomalies={current_anomalies}{anomaly_indicator} | batch={batch_id}")
        else:
            print(f"✗ write failed ({code}): {body.strip()} | batch={batch_id}")

        time.sleep(EMIT_INTERVAL_SEC)

except KeyboardInterrupt:
    print(f"\nStopping simulator… Total anomalies: {anomaly_count}")
finally:
    csv_fh.close()