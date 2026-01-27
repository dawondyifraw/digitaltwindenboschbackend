#!/usr/bin/env python3
# Simulated sensors -> InfluxDB v2
# - Adds ground-truth anomaly labels (anomaly=0/1, anomaly_type tag)
# - Writes per-sensor rows; logs HTTP write latency to CSV
# - No Telegraf/Kafka in this path

import time, random, math, requests, uuid, csv, os
from datetime import datetime
import math as pymath

INFLUXDB_URL = 'http://localhost:8086/api/v2/write'
ORG          = "DenBosch"
BUCKET       = "sensors_db"
TOKEN        = "vQZH5VT3VRUrDBdX4oyDO5BV7kWN4NvRvUJUSvkOGEz-cL3huzmpkBo5ywMVBioDXNQ0UfHc3afinUpxFnLmA=="

HEADERS = {'Authorization': f'Token {TOKEN}', 'Content-Type': 'text/plain'}

EMIT_INTERVAL_SEC   = 1.0
ANOMALY_PROBABILITY = 0.05  # 5%
random.seed(42)

CSV_FILE = "metrics_ingest.csv"
RUN_ID   = str(uuid.uuid4())

def ensure_csv_header(path):
    exists = os.path.exists(path)
    f = open(path, "a", newline="")
    w = csv.writer(f)
    if not exists:
        w.writerow([
            "run_id","batch_id","sensor_id","zone","latitude","longitude",
            "ts_ns","status","http_ms","anomaly","anomaly_type",
            "co2_ppm","no2_ppb","pm25_ugm3","noise_db"
        ])
        f.flush()
    return f, w

csv_fh, csv_writer = ensure_csv_header(CSV_FILE)

def now_ns():
    return int(time.time() * 1_000_000_000)

def lp_escape(v: str) -> str:
    return str(v).replace(' ', r'\ ').replace(',', r'\,')

def safe_num(x, lo=None, hi=None):
    try:
        v = float(x)
        if pymath.isnan(v) or pymath.isinf(v):
            return None
        if lo is not None: v = max(v, lo)
        if hi is not None: v = min(v, hi)
        return v
    except Exception:
        return None

def pick_anomaly_type():
    return random.choice(["co2_spike", "noise_burst"])

class Sensor:
    def __init__(self, sid, zone, lat, lon):
        self.id = sid; self.zone = zone; self.lat = lat; self.lon = lon

    def sample(self):
        t = time.time(); day = 86400.0
        phi = 2 * pymath.pi * ((t % day) / day)

        co2  = 500 + 200 * pymath.sin(phi)     + random.gauss(0, 15)
        no2  =  20 +  10 * pymath.sin(2 * phi) + random.gauss(0, 3)
        pm25 =   8 +   6 * pymath.sin(1.5*phi) + random.gauss(0, 2)

        base_noise = 65 if self.zone == "construction" else (45 if self.zone=="residential" else 55)
        noise = base_noise + 8 * pymath.sin(3 * phi) + random.gauss(0, 4)

        anomaly = 0; anomaly_type = ""
        if random.random() < ANOMALY_PROBABILITY:
            anomaly = 1
            anomaly_type = pick_anomaly_type()
            if anomaly_type == "co2_spike":
                co2 *= 1.8      # strong CO2 jump
                no2 *= 1.2
                pm25 *= 1.2
            else:  # noise_burst
                noise *= 1.4    # strong noise jump

        return {
            "co2_ppm": round(max(co2, 350), 2),
            "no2_ppb": round(max(no2, 1), 2),
            "pm25_ugm3": round(max(pm25, 1), 2),
            "noise_db": round(max(noise, 30), 1),
            "anomaly": anomaly,
            "anomaly_type": anomaly_type
        }

def to_line_protocol(sensor, fields, ts_ns, run_id, batch_id):
    # Ensure anomaly_type is never empty for tags
    an_type = fields.get("anomaly_type") or "none"

    tags = (
        f"sensor_id={lp_escape(sensor.id)},"
        f"zone={lp_escape(sensor.zone)},"
        f"run_id={lp_escape(run_id)},"
        f"batch_id={lp_escape(batch_id)},"
        f"anomaly_type={lp_escape(an_type)}"
    )

    # numeric fields
    co2   = safe_num(fields['co2_ppm'])
    no2   = safe_num(fields['no2_ppb'])
    pm25  = safe_num(fields['pm25_ugm3'])
    noise = safe_num(fields['noise_db'])
    if None in (co2, no2, pm25, noise):
        return ""  # drop invalid row

    anomaly = int(fields["anomaly"])

    # measurement, tags  fields                                  timestamp
    return (
        f"environment,{tags} "
        f"co2_ppm={co2},no2_ppb={no2},pm25_ugm3={pm25},noise_db={noise},"
        f"anomaly={anomaly},latitude={sensor.lat},longitude={sensor.lon} "
        f"{ts_ns}"
    )

SESSION = requests.Session()

def send_batch(lines, max_retries=2, backoff=0.3):
    if not lines: return 204, "", 0
    payload = "\n".join(lines)
    params = {'org': ORG, 'bucket': BUCKET, 'precision': 'ns'}
    attempt = 0
    while True:
        t0 = time.time()
        try:
            r = SESSION.post(INFLUXDB_URL, params=params, data=payload,
                             headers=HEADERS, timeout=5)
            http_ms = int((time.time() - t0) * 1000)
            return r.status_code, r.text, http_ms
        except requests.RequestException as e:
            http_ms = int((time.time() - t0) * 1000)
            if attempt >= max_retries:
                return 599, str(e), http_ms
            time.sleep(backoff * (attempt + 1))
            attempt += 1

sensors = [
    Sensor("S-C1","construction",51.6903,5.3030),
    Sensor("S-R1","residential", 51.6921,5.3075),
    Sensor("S-I1","industrial",  51.6867,5.2982),
]

print("Starting sensor simulator -> InfluxDB (no Telegraf)")
print(f"Run ID: {RUN_ID} | Sensors: {[s.id for s in sensors]} | Interval={EMIT_INTERVAL_SEC}s | Anomaly p={ANOMALY_PROBABILITY*100}%")

# connectivity check
ts = now_ns(); sample = sensors[0].sample()
test_line = to_line_protocol(sensors[0], sample, ts, RUN_ID, "b000000")
code, body, ms = send_batch([test_line])
if code != 204:
    print(f"✗ InfluxDB write failed ({code}): {body}")
    csv_fh.close(); raise SystemExit(1)
print(f"✓ InfluxDB OK (http_ms={ms}). Starting stream…")

batch_counter = 0
try:
    while True:
        batch_counter += 1
        batch_id = f"b{batch_counter:06d}"

        lines = []; sampled = []
        for s in sensors:
            fields = s.sample()
            ts_ns = now_ns()
            lp = to_line_protocol(s, fields, ts_ns, RUN_ID, batch_id)
            if lp: lines.append(lp)
            sampled.append((s, fields, ts_ns))

        code, body, http_ms = send_batch(lines)

        ok = (code == 204)
        for s, fields, ts_ns in sampled:
            csv_writer.writerow([
                RUN_ID, batch_id, s.id, s.zone, s.lat, s.lon,
                ts_ns, ("OK" if ok else f"ERR{code}"), http_ms, fields["anomaly"],
                fields["anomaly_type"], fields["co2_ppm"], fields["no2_ppb"], fields["pm25_ugm3"], fields["noise_db"]
            ])
        csv_fh.flush()

        current_anoms = sum(1 for _, f, _ in sampled if f['anomaly']==1)
        flag = " 🚨" if current_anoms else ""
        print(f"{'✓' if ok else '✗'} wrote {len(lines)} pts | http_ms={http_ms} | anomalies={current_anoms}{flag} | {batch_id}")

        time.sleep(EMIT_INTERVAL_SEC)

except KeyboardInterrupt:
    print("\nStopping simulator.")
finally:
    csv_fh.close()
