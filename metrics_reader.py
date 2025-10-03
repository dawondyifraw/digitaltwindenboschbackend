#!/usr/bin/env python3
import csv, sys, os, statistics
from collections import defaultdict
CSV_FILE = "metrics_ingest.csv"
CSV_PATH = os.getenv("CSV_PATH", CSV_FILE)

def load_rows(run_id=None):
    rows = []
    with open(CSV_PATH, newline="") as f:
        r = csv.DictReader(f)
        for row in r:
            if run_id and row["run_id"] != run_id:
                continue
            rows.append(row)
    return rows

def latest_run_id():
    rid = None
    try:
        with open(CSV_PATH, newline="") as f:
            r = csv.DictReader(f)
            for row in r:
                rid = row["run_id"]
    except FileNotFoundError:
        pass
    return rid

def pct(values, p):
    if not values: return float("nan")
    values = sorted(values)
    k = int(round((p/100.0)*(len(values)-1)))
    return values[k]

def main():
    if not os.path.exists(CSV_PATH):
        print(f"No CSV: {CSV_PATH}")
        sys.exit(1)

    run_id = sys.argv[1] if len(sys.argv) > 1 else latest_run_id()
    if not run_id:
        print("No runs found.")
        sys.exit(1)

    rows = load_rows(run_id)
    if not rows:
        print(f"No rows for run_id={run_id}")
        sys.exit(1)

    lats = []
    ok = 0
    tot = 0
    per_sensor = defaultdict(int)

    print(f"Available columns: {list(rows[0].keys())}")  # DEBUG

    for row in rows:
        tot += 1
        
        # FIXED: Use 'status' column instead of 'http_status'
        if row["status"] == "OK":
            ok += 1

        # FIXED: Use 'http_ms' column instead of 'batch_write_latency_ms'
        try:
            lats.append(float(row["http_ms"]))
        except (ValueError, KeyError):
            pass

        per_sensor[row["sensor_id"]] += 1

    sr = (100.0 * ok / tot) if tot else 0.0
    
    # Calculate statistics
    if lats:
        med = statistics.median(lats)
        p90 = pct(lats, 90)
        p95 = pct(lats, 95)
        p99 = pct(lats, 99)
        mx  = max(lats)
        mn  = min(lats)
        mean = sum(lats) / len(lats)
    else:
        med = p90 = p95 = p99 = mx = mn = mean = float("nan")

    print(f"=== SUMMARY for run_id={run_id} ===")
    print(f"Points: {tot} | Success: {ok} ({sr:.2f}%)")
    print(f"Write latency (ms): min={mn:.2f} median={med:.2f} mean={mean:.2f} p90={p90:.2f} p95={p95:.2f} p99={p99:.2f} max={mx:.2f}")
    print("\nPer-sensor counts:")
    for s,c in sorted(per_sensor.items()):
        print(f"  {s}: {c}")

if __name__ == "__main__":
    main()