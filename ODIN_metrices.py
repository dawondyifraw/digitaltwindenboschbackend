#!/usr/bin/env python3
import os, time, csv, json, random
from datetime import datetime
import requests
from statistics import mean
from datetime import datetime, timezone


ODIN_ENDPOINT = os.getenv("ODIN_ENDPOINT", "http://127.0.0.1:5050/query")
RUN_LABEL     = os.getenv("RUN_LABEL", "hybrid_v1")  # e.g., local_only, hybrid_v1

# Query set with simple categories for breakdowns
TEST_QUERIES = [
    ("lookup",      "What was the CO2 level in construction zone yesterday?"),
    ("timeseries",  "Show me noise levels for sensor S-I1 last hour"),
    ("aggregate",   "Compare PM2.5 across all zones last 24 hours"),
    ("aggregate",   "What was the maximum CO2 reading this week?"),
    ("aggregate",   "Show average noise levels by zone"),
    ("threshold",   "When did CO2 exceed 1000 ppm in construction zone?"),
    ("lookup",      "Latest CO2 readings from all sensors"),
    # Unsupported/off-scope (expect graceful handling)
    ("unsupported", "What is current temperature?"),
    ("invalid",     "Plot CO2 for sensor XYZ"),
    ("unsupported", "How is the weather today?")
]

CSV_PATH = os.getenv("CSV_PATH", "odin_bench.csv")

def percentile(values, p):
    if not values: return float("nan")
    values = sorted(values)
    k = (len(values)-1) * (p/100.0)
    f = int(k); c = min(f+1, len(values)-1)
    if f == c: return float(values[f])
    return float(values[f] + (values[c]-values[f])*(k-f))

def call_odin(query: str):
    payload = {"query": query, "timestamp":  datetime.now(timezone.utc).isoformat() }
    t0 = time.time()
    r  = requests.post(ODIN_ENDPOINT, json=payload, headers={"Content-Type":"application/json"}, timeout=30)
    dt_ms = (time.time() - t0) * 1000.0

    # Try structured validity first (preferred)
    valid = None
    engine = None
    preview = ""
    try:
        data = r.json()
        preview = json.dumps(data)[:200]
        # If your backend includes flags like these, use them:
        valid  = data.get("valid")            # bool or None
        engine = data.get("engine")           # "local" | "hybrid" | None
    except Exception:
        preview = (r.text or "")[:200]

    # Fallback heuristic (only if valid is still None)
    if valid is None:
        rt = (preview or "").lower()
        bad = any(s in rt for s in ["error", "invalid", "not found", "no data", "off-topic"])
        valid = not bad

    return r.status_code, dt_ms, valid, engine, preview

def warmup(n=2):
    for _ in range(n):
        try:
            call_odin("ping")
        except Exception:
            pass
        time.sleep(0.1)

def main():
    print("Starting ODIN Performance Measurement...")
    print(f"Endpoint: {ODIN_ENDPOINT} | Run label: {RUN_LABEL}")

    # Warm-up
    warmup()

    rows = []
    for i, (qtype, query) in enumerate(TEST_QUERIES, 1):
        print(f"\n--- Testing Query {i}/{len(TEST_QUERIES)} ---")
        print(f"[{qtype}] {query}")

        try:
            code, rt_ms, valid, engine, preview = call_odin(query)
            status = "success" if code == 200 else f"http_{code}"
        except requests.exceptions.Timeout:
            code, rt_ms, valid, engine, preview = 0, 30000.0, False, None, "timeout"
            status = "timeout"
        except Exception as e:
            code, rt_ms, valid, engine, preview = 0, 0.0, False, None, f"exception: {e}"
            status = "exception"

        print(f"Response time: {rt_ms:.0f}ms | Valid: {valid} | Status: {status} | Engine: {engine or '-'}")

        rows.append({
            "ts": datetime.now(timezone.utc).isoformat() ,
            "run_label": RUN_LABEL,
            "idx": i,
            "type": qtype,
            "query": query,
            "status_code": code,
            "latency_ms": round(rt_ms, 2),
            "valid": bool(valid),
            "engine": engine or "",
            "preview": preview
        })

        # Small jitter between calls (avoid burst artifacts)
        time.sleep(0.2 + random.random()*0.1)

    # Persist CSV (append/create)
    write_header = not os.path.exists(CSV_PATH)
    with open(CSV_PATH, "a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        if write_header: w.writeheader()
        w.writerows(rows)

    # Compute summary
    lats = [r["latency_ms"] for r in rows if r["latency_ms"] > 0]
    ok   = [r for r in rows if r["valid"]]
    http_ok = [r for r in rows if r["status_code"] == 200]

    print("\n" + "="*50)
    print("ODIN PERFORMANCE RESULTS")
    print("="*50)
    print(f"Total Queries Tested: {len(rows)}")
    print(f"HTTP 200 rate: {len(http_ok)/len(rows)*100:.1f}%")
    print(f"First-Pass Validity: {len(ok)/len(rows)*100:.1f}%")
    if lats:
        print(f"Average Response Time: {mean(lats):.0f}ms")
        print(f"Min / Max: {min(lats):.0f} / {max(lats):.0f} ms")
        print(f"p50/p90/p95/p99: {percentile(lats,50):.0f} / {percentile(lats,90):.0f} / {percentile(lats,95):.0f} / {percentile(lats,99):.0f} ms")

    # Per-type breakdown
    print("\nPer-type breakdown (valid % | p50 ms):")
    types = sorted(set(t for t,_ in TEST_QUERIES))
    for t in types:
        subset = [r for r in rows if r["type"] == t]
        val_ok = [r for r in subset if r["valid"]]
        l = [r["latency_ms"] for r in subset if r["latency_ms"] > 0]
        p50 = percentile(l,50) if l else float("nan")
        print(f" - {t:11s}: {len(val_ok)/len(subset)*100:5.1f}% | {p50:5.0f} ms")

    print(f"\nCSV written: {CSV_PATH}")

if __name__ == "__main__":
    main()
