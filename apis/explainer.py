#!/usr/bin/env python3
"""
ODIN (Hybrid) — Local intent + LLM fallback for Flux query construction.
AUTHOR: Daniel Wondyifraw DataTwinLabs.nl

Schema (as produced by your direct-to-Influx writer):
  bucket:       sensordb_influx
  measurement:  environment
  tags:         sensor_id, zone
  fields:       co2_ppm, no2_ppb, pm25_ugm3, noise_db, latitude, longitude
"""

import re
import json
import logging
import requests
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional
from influxdb_client import InfluxDBClient

import os
import re
import json
import logging
import requests
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional
from influxdb_client import InfluxDBClient

# =============== CONFIG ===============
HYPERBOLIC_URL = os.getenv("HYPERBOLIC_URL", "https://api.hyperbolic.xyz/v1/chat/completions")
#HYPERBOLIC_URL = "http://localhost:11434/v1/chat/completions"  # Update the port/path as needed.

LOCAL_URL = "http://ollama:11434"

HYPERBOLIC_API_KEY = os.getenv("HYPERBOLIC_API_KEY", "")  # Load from environment
INFLUX_URL = os.getenv("INFLUX_URL", "http://localhost:8086")
INFLUX_TOKEN = os.getenv("INFLUX_TOKEN", "")  # Load from environment
INFLUX_ORG = os.getenv("INFLUX_ORG", "DenBosch")
KADASTER_API_URL = "https://api.pdok.nl/kadaster/brk-percelen/v1/percelen"
LOCATIESERVER_URL = "https://geodata.nationaalgeoregister.nl/locatieserver/v3/search"
BUCKET_NAME = "sensors_db"
BUCKET_NAME = "sensors_db"
MEASUREMENT_NAME = "kafka_consumer"
SENSOR_FIELDS = ["co2", "sound_level", "no2"]
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

BUCKET       = "sensors_db"   # <-- match your producer
MEASUREMENT  = "environment"

FIELDS_NUMERIC = ["co2_ppm", "no2_ppb", "pm25_ugm3", "noise_db"]
FIELDS_COORDS  = ["latitude", "longitude"]

# LLM (fallback only)
HYPERBOLIC_URL     = "https://api.hyperbolic.xyz/v1/chat/completions"

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")

# ---------------- Utilities ----------------
def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def sanitize_flux(q: str) -> str:
    q = q.strip().replace("’","'").replace("‘","'").replace("“",'"').replace("”",'"').replace("\u00A0", " ")
    q = " ".join(q.splitlines())
    q = re.sub(r'from\(\s*bucket:\s*\'([^\']+)\'\s*\)', r'from(bucket: "\1")', q)
    q = "".join(ch for ch in q if ord(ch) < 128)
    return q

def is_offtopic(query: str) -> bool:
    uq = query.lower()
    relevant = [
        "co2", "noise", "sound", "no2", "pm2.5", "pm25", "ppm", "ppb", "ug/m3",
        "sensor", "zone", "average", "mean", "max", "min", "latest",
        "last hour", "last 24 hours", "last day", "yesterday", "this week", "week"
    ]
    return not any(t in uq for t in relevant)

# ---------------- Intent Parser ----------------
_TIME_WINDOWS = [
    (r"\blast\s*30\s*min(ute)?s?\b", "-30m"),
    (r"\blast\s*hour\b|\blast\s*1\s*hour\b", "-1h"),
    (r"\blast\s*24\s*hours\b|\blast\s*day\b", "-24h"),
    (r"\byesterday\b", "-48h"),     # conservative; we still return the last 48h window
    (r"\bthis\s*week\b", "-7d"),
    (r"\blast\s*7\s*days\b", "-7d"),
]
_METRIC_ALIASES = {
    "co2": "co2_ppm",
    "noise": "noise_db",
    "sound": "noise_db",
    "no2": "no2_ppb",
    "pm2.5": "pm25_ugm3",
    "pm25": "pm25_ugm3",
}

_SENSOR_PATTERNS = [
    r"\bsensor\s*([A-Za-z0-9\-_.]+)\b",
    r"\bS-[CRI]\d+\b",  # S-C1 / S-R1 / S-I1
]

def parse_intent(q: str) -> Dict[str, Any]:
    uq = q.lower()

    # metrics
    metrics: List[str] = []
    for k, v in _METRIC_ALIASES.items():
        if re.search(rf"\b{k}\b", uq):
            metrics.append(v)
    metrics = list(dict.fromkeys(metrics))

    # sensor_id
    sensor_id = None
    for pat in _SENSOR_PATTERNS:
        m = re.search(pat, q)  # keep case
        if m:
            sensor_id = m.group(0) if len(m.groups()) == 0 else m.group(1)
            break

    # zone
    zone = None
    for z in ["construction", "residential", "industrial"]:
        if re.search(rf"\b{z}\b", uq):
            zone = z
            break

    # time window
    window = "-24h"
    for rx, val in _TIME_WINDOWS:
        if re.search(rx, uq):
            window = val
            break

    # aggregations
    agg = None
    if re.search(r"\bavg|average|mean\b", uq):
        agg = "mean"
    elif re.search(r"\bmax(imum)?\b", uq):
        agg = "max"
    elif re.search(r"\bmin(imum)?\b", uq):
        agg = "min"

    # thresholds
    threshold = None
    tm = re.search(r"\b(co2|no2|pm2\.?5|pm25|noise|sound)\s*(>|>=|exceed(s|ed)?)\s*([0-9]+(\.[0-9]+)?)\s*(ppm|ppb|ug/m3|db)?", uq)
    if tm:
        raw_metric = tm.group(1).replace(".", "")
        value = float(tm.group(4))
        metric = _METRIC_ALIASES.get(raw_metric, None)
        if metric:
            threshold = {"metric": metric, "op": ">", "value": value}

    latest = bool(re.search(r"\blatest\b|\bmost recent\b", uq))

    # classify
    qtype = "timeseries"
    if threshold:
        qtype = "threshold"
    elif agg:
        qtype = "aggregate"
    elif latest:
        qtype = "lookup_latest"

    return {
        "type": qtype,
        "metrics": metrics,           # [] means “any numeric”
        "sensor_id": sensor_id,
        "zone": zone,
        "window": window,
        "agg": agg,
        "threshold": threshold,
        "latest": latest,
    }

# ---------------- Flux builders (local) ----------------
def _tag_filters(sensor_id: Optional[str], zone: Optional[str]) -> str:
    parts = []
    if sensor_id:
        parts.append(f'|> filter(fn: (r) => r["sensor_id"] == "{sensor_id}")')
    if zone:
        parts.append(f'|> filter(fn: (r) => r["zone"] == "{zone}")')
    return " ".join(parts) + (" " if parts else "")

def _field_filter(metrics: List[str]) -> str:
    fields = metrics if metrics else FIELDS_NUMERIC
    ors = " or ".join([f'r._field == "{f}"' for f in fields])
    return f'|> filter(fn: (r) => {ors}) '

def _pivot_keep(metrics: List[str]) -> str:
    cols = metrics if metrics else FIELDS_NUMERIC + FIELDS_COORDS
    keep_cols = ["_time"] + cols + ["sensor_id","zone"]
    keep_list = ", ".join([f'"{c}"' for c in keep_cols])
    return f'|> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value") |> keep(columns: [{keep_list}]) '

def build_flux_from_intent(intent: Dict[str, Any]) -> Optional[str]:
    itype     = intent["type"]
    metrics   = intent["metrics"]
    sensor_id = intent["sensor_id"]
    zone      = intent["zone"]
    window    = intent["window"]
    agg       = intent["agg"]
    threshold = intent["threshold"]
    latest    = intent["latest"]

    # Base
    base = f'from(bucket: "{BUCKET}") |> range(start: {window}) |> filter(fn: (r) => r._measurement == "{MEASUREMENT}") '

    # --- 1) TIMESERIES (raw series, no pivot) ---
    if itype == "timeseries" and not agg and not threshold and metrics:
        # If user gave a single metric + (optional) sensor filter → classic series
        # Example query: "noise for sensor S-I1 last hour"
        flux = base
        flux += _field_filter(metrics)
        flux += _tag_filters(sensor_id, zone)
        # group per sensor so we don't mix series
        flux += '|> group(columns: ["sensor_id","_field"]) '
        # keep tidy columns
        flux += '|> keep(columns: ["_time","_value","_field","sensor_id","zone"]) '
        return sanitize_flux(flux)

    # --- 2) LOOKUP_LATEST across sensors (per-sensor latest) ---
    if itype == "lookup_latest" and (metrics or True):
        # choose a single primary metric for “latest”; default to co2_ppm if none
        primary = metrics[0] if metrics else "co2_ppm"
        flux = base
        flux += f'|> filter(fn: (r) => r._field == "{primary}") '
        flux += _tag_filters(sensor_id, zone)
        flux += '|> group(columns: ["sensor_id","_field"]) '
        flux += '|> sort(columns: ["_time"], desc: true) '
        flux += '|> unique(column: "sensor_id") '
        # now one row per sensor; keep columns
        flux += '|> keep(columns: ["_time","_value","sensor_id","zone"]) '
        return sanitize_flux(flux)

    # --- 3) AGGREGATE over window (pivot to wide, then aggregateWindow) ---
    if itype == "aggregate" and agg in ["mean","max","min"]:
        fn = {"mean":"mean","max":"max","min":"min"}[agg]
        flux = base
        flux += _field_filter(metrics)
        flux += _tag_filters(sensor_id, zone)
        # 1h window aggregation for stability
        flux += f'|> aggregateWindow(every: 1h, fn: {fn}, createEmpty: false) '
        flux += _pivot_keep(metrics)
        return sanitize_flux(flux)

    # --- 4) THRESHOLD exceedances (pivot so we can filter on a named field) ---
    if itype == "threshold" and threshold and threshold["metric"]:
        m = threshold["metric"]; v = threshold["value"]
        # Pull all numeric fields so pivot has the metric present
        flux = base
        flux += _field_filter([m])  # narrow to the relevant field for performance
        flux += _tag_filters(sensor_id, zone)
        flux += _pivot_keep([m])
        flux += f'|> filter(fn: (r) => exists r["{m}"] and r["{m}"] > {v}) '
        return sanitize_flux(flux)

    # --- 5) Fallback “wide latest” (pivot + global latest) ---
    if latest:
        flux = base + _field_filter(metrics)
        flux += _tag_filters(sensor_id, zone)
        flux += _pivot_keep(metrics)
        flux += '|> sort(columns: ["_time"], desc: true) |> limit(n: 1) '
        return sanitize_flux(flux)

    # If nothing matched, return None to trigger LLM fallback
    return None

# ---------------- LLM Fallback ----------------
def llm_flux_fallback(user_query: str) -> Optional[str]:
    sys_prompt = f"""
Return a SINGLE-LINE InfluxDB Flux query for this schema:
- bucket: {BUCKET}
- measurement: {MEASUREMENT}
- tags: sensor_id, zone
- fields: {", ".join(FIELDS_NUMERIC + FIELDS_COORDS)}
Rules:
1) Start with from(bucket: "{BUCKET}") |> range(start: -24h)
2) Always filter _measurement == "{MEASUREMENT}"
3) If a sensor or zone is mentioned, filter r["sensor_id"] / r["zone"].
4) For raw series, do NOT pivot; keep _time/_value/_field/sensor_id/zone.
5) For per-sensor latest, group by sensor_id then sort desc by _time and unique(sensor_id).
6) One line only, no comments or code fences.
"""
    payload = {
        "model": "deepseek-ai/DeepSeek-V3",
        "max_tokens": 500,
        "temperature": 0.0,
        "top_p": 1.0,
        "messages": [
            {"role": "system", "content": sys_prompt},
            {"role": "user", "content": user_query}
        ]
    }
    headers = {"Authorization": f"Bearer {HYPERBOLIC_API_KEY}", "Content-Type": "application/json"}
    try:
        r = requests.post(HYPERBOLIC_URL, json=payload, headers=headers, timeout=20)
        r.raise_for_status()
        flux = r.json().get("choices", [{}])[0].get("message", {}).get("content", "")
        return sanitize_flux(flux)
    except Exception as e:
        logging.error(f"LLM fallback error: {e}")
        return None

# ---------------- Main handler ----------------
def handle_query(user_query: str) -> str:
    if is_offtopic(user_query):
        return ("Out of scope for CO₂/NO₂/PM2.5/noise data. "
                "Try: 'Show noise for sensor S-I1 last hour'.")

    intent = parse_intent(user_query)
    flux = build_flux_from_intent(intent)

    if not flux:
        flux = llm_flux_fallback(user_query)
    if not flux:
        return "I couldn't construct a valid Flux query for that request."

    logging.info(f"Flux: {flux}")

    try:
        client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
        tables = client.query_api().query(query=flux)
    except Exception as e:
        return f"InfluxDB query failed: {e}"

    rows: List[Dict[str, Any]] = []
    for table in tables:
        for rec in table.records:
            vals = rec.values
            t = rec.get_time() or vals.get("_time")
            if hasattr(t, "isoformat"):
                t = t.isoformat()
            rows.append({
                "time": t,
                "sensor_id": vals.get("sensor_id"),
                "zone": vals.get("zone"),
                "co2_ppm": vals.get("co2_ppm"),
                "no2_ppb": vals.get("no2_ppb"),
                "pm25_ugm3": vals.get("pm25_ugm3"),
                "noise_db": vals.get("noise_db"),
                "latitude": vals.get("latitude"),
                "longitude": vals.get("longitude"),
                "_field": vals.get("_field"),
                "_value": vals.get("_value"),
            })

    if not rows:
        return "No data matched your request."

    # Quick human summary
    r0 = rows[0]
    summary = f"Returned {len(rows)} row(s). Example @ {r0.get('time')} sensor={r0.get('sensor_id')} zone={r0.get('zone')}"
    parts = []
    for f in ["co2_ppm", "no2_ppb", "pm25_ugm3", "noise_db"]:
        if r0.get(f) is not None:
            parts.append(f"{f}={r0[f]}")
    if parts:
        summary += " | " + ", ".join(parts)
    return summary

# ---------------- CLI quick test ----------------
if __name__ == "__main__":
    q = "Show me noise for sensor S-I1 last hour"
    print("Q:", q)
    print(handle_query(q))
