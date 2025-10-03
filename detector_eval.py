#!/usr/bin/env python3
import os, time, csv, json, asyncio
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Tuple
from influxdb_client import InfluxDBClient
try:
    import websockets  # optional
except ImportError:
    websockets = None

INFLUX_URL   = "http://localhost:8086"
INFLUX_ORG   = "DenBosch"
INFLUX_TOKEN = "vQZH5VT3VRUrDBdX4oyDO5BV7kWN4NvRvUJUSvkOGEz-cL3huzmpkBo5ywMVBioDXNQ0UfHc3afinUpxFnLmA=="
BUCKET       = "sensors_db"

# thresholds for simple rule-based detector
CO2_THRESH   = 750.0    # Slightly lower to catch more weak anomalies
NOISE_THRESH = 68.0     # Adjust based on which zones have FPs

# sliding window for polling & evaluation
WINDOW_SEC   = 10
POLL_EVERY   = 1.0

# optional WebSocket broadcast (set to ws://host:port/path or leave empty)
WS_ENDPOINT  = os.getenv("WS_ALERT_URL", "ws://localhost:6790")

CSV_OUT = "detector_eval.csv"

def iso_utc(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.isoformat()

def now_utc() -> datetime:
    return datetime.now(timezone.utc)

def ensure_csv(path: str):
    exists = os.path.exists(path)
    fh = open(path, "a", newline="")
    wr = csv.writer(fh)
    if not exists:
        wr.writerow([
            "t_end_iso","window_sec",
            "tp","fp","fn","precision","recall","f1",
            "detections","gt_anomalies"
        ])
        fh.flush()
    return fh, wr

def f1_score(p, r):
    return (2*p*r/(p+r)) if (p+r) > 0 else 0.0

def fmt2(x): return f"{x:.2f}"
# Add to your detector
CO2_ANOMALY_BUFFER = []

def detect_rules(row: Dict) -> List[str]:
    hits = []
    co2 = row.get("co2_ppm")
    noise = row.get("noise_db")
    zone = row.get("zone", "")
    
    # CO2 detection (same for all zones)
    if co2 is not None and co2 > 750.0:
        hits.append("co2_spike")
    
    # Zone-specific noise thresholds
    if noise is not None:
        if zone == "construction" and noise > 75.0:
            hits.append("noise_burst")
        elif zone == "industrial" and noise > 70.0:
            hits.append("noise_burst")
        elif zone == "residential" and noise > 65.0:  # Higher threshold for residential
            hits.append("noise_burst")
    
    return hits

async def ws_send(msg: Dict):
    if not WS_ENDPOINT or websockets is None:
        print("❌ WebSocket disabled or websockets not available")
        return
        
    try:
        print(f"🔗 Connecting to {WS_ENDPOINT}...")
        async with websockets.connect(WS_ENDPOINT) as ws:
            message = json.dumps(msg)
            print(f"📤 Sending {len(message)} bytes: {message[:100]}...")
            await ws.send(message)
            print("✅ Message sent successfully")
    except Exception as e:
        print(f"❌ WebSocket error: {e}")

def build_flux(window_sec: int) -> str:
    return f'''
from(bucket: "{BUCKET}")
  |> range(start: -{window_sec}s)
  |> filter(fn: (r) => r._measurement == "environment")
  |> pivot(rowKey:["_time"], columnKey:["_field"], valueColumn:"_value")
  |> keep(columns: ["_time","sensor_id","zone","run_id","batch_id","anomaly_type",
                    "co2_ppm","no2_ppb","pm25_ugm3","noise_db","anomaly","latitude","longitude"])
'''

def main():
    fh, wr = ensure_csv(CSV_OUT)
    client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
    qapi   = client.query_api()

    print(f"Detector running | window={WINDOW_SEC}s | poll={POLL_EVERY}s | co2>{CO2_THRESH} | noise>{NOISE_THRESH}")
    print(f"CSV -> {CSV_OUT} | WS -> {WS_ENDPOINT or '(disabled)'}")

    try:
        while True:
            t_end = now_utc()
            flux = build_flux(WINDOW_SEC)
            try:
                tables = qapi.query(query=flux)
            except Exception as e:
                print(f"Influx query error: {e}")
                time.sleep(POLL_EVERY); continue

            rows: List[Dict] = []
            for tbl in tables:
                for rec in tbl.records:
                    v = rec.values
                    rows.append({
                        "time": v.get("_time"),
                        "sensor_id": v.get("sensor_id"),
                        "zone": v.get("zone"),
                        "run_id": v.get("run_id"),
                        "batch_id": v.get("batch_id"),
                        "anomaly": v.get("anomaly"),
                        "anomaly_type": v.get("anomaly_type"),
                        "co2_ppm": v.get("co2_ppm"),
                        "noise_db": v.get("noise_db"),
                        "latitude": v.get("latitude"),
                        "longitude": v.get("longitude"),
                    })

            # ground-truth anomalies in window
            gt: List[Tuple[str,str,str]] = []  # (sensor_id, anomaly_type, iso_time)
            for r in rows:
                if r.get("anomaly") == 1 and r.get("anomaly_type"):
                    gt.append((r["sensor_id"], r["anomaly_type"], iso_utc(r["time"])))

            # detections by rules
            det: List[Tuple[str,str,str]] = []
            alert_msgs: List[Dict] = []
            for r in rows:
                hits = detect_rules(r)
                for kind in hits:
                    det.append((r["sensor_id"], kind, iso_utc(r["time"])))
                    alert_msgs.append({
                        "type": "ALERT",
                        "when": iso_utc(r["time"]),
                        "sensor_id": r["sensor_id"],
                        "zone": r["zone"],
                        "kind": kind,
                        "co2_ppm": r.get("co2_ppm"),
                        "noise_db": r.get("noise_db"),
                        "lat": r.get("latitude"),
                        "lon": r.get("longitude"),
                    })

            # matching (simple exact-class match per sensor within the same window)
            gt_set  = set((s,k,t) for s,k,t in gt)
            det_set = set((s,k,t) for s,k,t in det)

            # Relaxed match: ignore timestamp exactness, match on sensor+type in window
            gt_keys  = set((s,k) for (s,k,_) in gt)
            det_keys = set((s,k) for (s,k,_) in det)

            tp = len(gt_keys & det_keys)
            fp = len(det_keys - gt_keys)
            fn = len(gt_keys - det_keys)

            precision = tp / (tp+fp) if (tp+fp)>0 else 0.0
            recall    = tp / (tp+fn) if (tp+fn)>0 else 0.0
            f1        = f1_score(precision, recall)

            # Emit alerts (optional WS)
            if alert_msgs:
                print(f"[{iso_utc(t_end)}] Alerts: {len(alert_msgs)} | TP={tp} FP={fp} FN={fn} P={fmt2(precision)} R={fmt2(recall)} F1={fmt2(f1)}")
                if WS_ENDPOINT and websockets is not None:
                    try:
                        asyncio.get_event_loop().run_until_complete(ws_send({"batch": alert_msgs}))
                    except Exception:
                        pass
            else:
                print(f"[{iso_utc(t_end)}] No alerts | TP={tp} FP={fp} FN={fn} P={fmt2(precision)} R={fmt2(recall)} F1={fmt2(f1)}")

            # Log to CSV
            wr.writerow([
                iso_utc(t_end), WINDOW_SEC, tp, fp, fn,
                f"{precision:.4f}", f"{recall:.4f}", f"{f1:.4f}",
                len(det_keys), len(gt_keys)
            ])
            fh.flush()

            time.sleep(POLL_EVERY)

    except KeyboardInterrupt:
        print("\nDetector stopped.")
    finally:
        client.close()
        fh.close()

if __name__ == "__main__":
    main()