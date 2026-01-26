#!/usr/bin/env python3
"""
ODIN Chat Service
AUTHOR: Daniel Wondyifraw DataTwinLabs.nl
-----------------
Hybrid assistant:

1) If the user asks about CO₂ / NO₂ / PM2.5 / noise / sensors:
   - Parse intent locally
   - Build Flux query
   - Query InfluxDB
   - Return short, numeric summary + optional data

2) If not a sensor question:
   - Use LLM as a regular chatbot (urban digital twin / city assistant persona)

Dependencies:
    pip install flask flask-cors requests influxdb-client
"""

import re
import json
import logging
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional

import requests
from flask import Flask, request, jsonify
from flask_cors import CORS
from influxdb_client import InfluxDBClient

import os
import re
import json
import logging
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional

import requests
from flask import Flask, request, jsonify
from flask_cors import CORS
from influxdb_client import InfluxDBClient

# ===================== CONFIG =====================

# InfluxDB
INFLUX_URL   = os.getenv("INFLUX_URL", "http://localhost:8086")
INFLUX_TOKEN = os.getenv("INFLUX_TOKEN", "")  # Load from environment
INFLUX_ORG   = os.getenv("INFLUX_ORG", "DenBosch")
BUCKET       = os.getenv("BUCKET", "sensors_db")
MEASUREMENT  = "environment"

# LLM (Hyperbolic)
HYPERBOLIC_URL     = os.getenv("HYPERBOLIC_URL", "https://api.hyperbolic.xyz/v1/chat/completions")
HYPERBOLIC_API_KEY = os.getenv("HYPERBOLIC_API_KEY", "")  # Load from environment
LLM_MODEL          = os.getenv("LLM_MODEL", "deepseek-ai/DeepSeek-V3")

# Fields
FIELDS_NUMERIC = ["co2_ppm", "no2_ppb", "pm25_ugm3", "noise_db"]
FIELDS_COORDS  = ["latitude", "longitude"]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

# ===================== APP =====================

app = Flask(__name__)
CORS(app)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def sanitize_flux(q: str) -> str:
    q = q.strip().replace("’", "'").replace("‘", "'").replace("“", '"').replace("”", '"').replace("\u00A0", " ")
    q = " ".join(q.splitlines())
    q = re.sub(r'from\(\s*bucket:\s*\'([^\']+)\'\s*\)', r'from(bucket: "\1")', q)
    q = "".join(ch for ch in q if ord(ch) < 128)
    return q


def is_sensor_query(query: str) -> bool:
    uq = query.lower()
    relevant = [
        "co2", "noise", "sound", "no2", "pm2.5", "pm25",
        "ppm", "ppb", "ug/m3", "µg/m3",
        "sensor", "zone", "construction", "residential", "industrial",
        "average", "mean", "max", "min", "latest", "threshold",
        "last hour", "last 24 hours", "last day", "this week", "last 7 days"
    ]
    return any(t in uq for t in relevant)


# ===================== INTENT PARSER =====================

_TIME_WINDOWS = [
    (r"\blast\s*30\s*min(ute)?s?\b", "-30m"),
    (r"\blast\s*hour\b|\blast\s*1\s*hour\b", "-1h"),
    (r"\blast\s*24\s*hours\b|\blast\s*day\b", "-24h"),
    (r"\byesterday\b", "-48h"),
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
    r"\bS-[CRI]\d+\b",
]


def parse_intent(q: str) -> Dict[str, Any]:
    uq = q.lower()

    # metrics
    metrics: List[str] = []
    for k, v in _METRIC_ALIASES.items():
        if re.search(rf"\b{k}\b", uq):
            metrics.append(v)
    metrics = list(dict.fromkeys(metrics))

    # sensor_id (preserve case if user typed S-I1 etc)
    sensor_id = None
    for pat in _SENSOR_PATTERNS:
        m = re.search(pat, q)
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

    # threshold
    threshold = None
    tm = re.search(
        r"\b(co2|no2|pm2\.?5|pm25|noise|sound)\s*(>|>=|exceed(s|ed)?)\s*([0-9]+(\.[0-9]+)?)\s*(ppm|ppb|ug/m3|µg/m3|db)?",
        uq
    )
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
        "metrics": metrics,
        "sensor_id": sensor_id,
        "zone": zone,
        "window": window,
        "agg": agg,
        "threshold": threshold,
        "latest": latest,
    }


# ===================== FLUX BUILDERS =====================

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
    keep_cols = ["_time"] + cols + ["sensor_id", "zone"]
    keep_list = ", ".join([f'"{c}"' for c in keep_cols])
    return (
        '|> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value") '
        f'|> keep(columns: [{keep_list}]) '
    )


def build_flux_from_intent(intent: Dict[str, Any]) -> Optional[str]:
    itype     = intent["type"]
    metrics   = intent["metrics"]
    sensor_id = intent["sensor_id"]
    zone      = intent["zone"]
    window    = intent["window"]
    agg       = intent["agg"]
    threshold = intent["threshold"]
    latest    = intent["latest"]

    base = (
        f'from(bucket: "{BUCKET}") '
        f'|> range(start: {window}) '
        f'|> filter(fn: (r) => r._measurement == "{MEASUREMENT}") '
    )

    # 1) Timeseries for explicit metric
    if itype == "timeseries" and not agg and not threshold and metrics:
        flux = base
        flux += _field_filter(metrics)
        flux += _tag_filters(sensor_id, zone)
        flux += '|> group(columns: ["sensor_id","_field"]) '
        flux += '|> keep(columns: ["_time","_value","_field","sensor_id","zone"]) '
        return sanitize_flux(flux)

    # 2) Per-sensor latest
    if itype == "lookup_latest":
        primary = metrics[0] if metrics else "co2_ppm"
        flux = base
        flux += f'|> filter(fn: (r) => r._field == "{primary}") '
        flux += _tag_filters(sensor_id, zone)
        flux += '|> group(columns: ["sensor_id","_field"]) '
        flux += '|> sort(columns: ["_time"], desc: true) '
        flux += '|> unique(column: "sensor_id") '
        flux += '|> keep(columns: ["_time","_value","sensor_id","zone"]) '
        return sanitize_flux(flux)

    # 3) Aggregate over window
    if itype == "aggregate" and agg in ["mean", "max", "min"]:
        fn = agg
        flux = base
        flux += _field_filter(metrics)
        flux += _tag_filters(sensor_id, zone)
        flux += f'|> aggregateWindow(every: 1h, fn: {fn}, createEmpty: false) '
        flux += _pivot_keep(metrics)
        return sanitize_flux(flux)

    # 4) Threshold exceedance
    if itype == "threshold" and threshold and threshold["metric"]:
        m = threshold["metric"]
        v = threshold["value"]
        flux = base
        flux += _field_filter([m])
        flux += _tag_filters(sensor_id, zone)
        flux += _pivot_keep([m])
        flux += f'|> filter(fn: (r) => exists r["{m}"] and r["{m}"] > {v}) '
        return sanitize_flux(flux)

    # 5) Fallback wide-latest
    if latest:
        flux = base
        flux += _field_filter(metrics)
        flux += _tag_filters(sensor_id, zone)
        flux += _pivot_keep(metrics)
        flux += '|> sort(columns: ["_time"], desc: true) |> limit(n: 1) '
        return sanitize_flux(flux)

    return None


# ===================== LLM HELPERS =====================

def call_llm(system_prompt: str, messages: List[Dict[str, str]]) -> str:
    payload = {
        "model": LLM_MODEL,
        "max_tokens": 600,
        "temperature": 0.1,
        "top_p": 1.0,
        "messages": [{"role": "system", "content": system_prompt}] + messages
    }
    headers = {
        "Authorization": f"Bearer {HYPERBOLIC_API_KEY}",
        "Content-Type": "application/json"
    }
    r = requests.post(HYPERBOLIC_URL, json=payload, headers=headers, timeout=30)
    r.raise_for_status()
    return r.json().get("choices", [{}])[0].get("message", {}).get("content", "").strip()


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
    try:
        flux = call_llm(sys_prompt, [{"role": "user", "content": user_query}])
        return sanitize_flux(flux)
    except Exception as e:
        logging.error(f"LLM fallback error: {e}")
        return None


# ===================== INFLUX QUERY + SUMMARY =====================

def query_influx(flux: str) -> List[Dict[str, Any]]:
    client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
    tables = client.query_api().query(query=flux)

    rows: List[Dict[str, Any]] = []
    for table in tables:
        for rec in table.records:
            vals = rec.values
            t = rec.get_time() or vals.get("_time")
            if hasattr(t, "isoformat"):
                t = t.isoformat()

            field_name = vals.get("_field")
            value = vals.get("_value")

            row: Dict[str, Any] = {
                "time": t,
                "sensor_id": vals.get("sensor_id"),
                "zone": vals.get("zone"),
                "latitude": vals.get("latitude"),
                "longitude": vals.get("longitude"),
                "_field": field_name,
                "_value": value,
            }

            # Map series rows into named columns
            if field_name in FIELDS_NUMERIC + FIELDS_COORDS:
                row[field_name] = value

            # For pivoted rows, values are already under the field names
            for f in FIELDS_NUMERIC + FIELDS_COORDS:
                if f in vals and isinstance(vals[f], (int, float, float)):
                    row.setdefault(f, vals[f])

            rows.append(row)

    return rows


def simple_summary(intent: Dict[str, Any], rows: List[Dict[str, Any]]) -> str:
    if not rows:
        return "No data matched your request."

    itype = intent.get("type")
    metrics = intent.get("metrics") or []
    main_metric = metrics[0] if metrics else None

    if main_metric:
        numeric_rows = [r for r in rows if isinstance(r.get(main_metric), (int, float))]
        if not numeric_rows:
            return f"{len(rows)} measurements, but no numeric values for {main_metric}."

        vals = [r[main_metric] for r in numeric_rows]
        latest = numeric_rows[-1]
        latest_value = latest.get(main_metric)
        sensor = latest.get("sensor_id") or "an unknown sensor"
        zone = latest.get("zone") or "unknown zone"

        if itype == "aggregate":
            avg = sum(vals) / len(vals)
            return f"{len(numeric_rows)} measurements for {main_metric} in {zone}. Average is {avg:.1f}."
        elif intent.get("threshold"):
            thr = intent["threshold"]
            return (
                f"{len(numeric_rows)} measurements where {main_metric} exceeds {thr['value']}. "
                f"Latest exceedance is {latest_value} at {sensor} ({zone})."
            )
        else:
            return f"{len(numeric_rows)} measurements for {main_metric}. Latest value is {latest_value} at {sensor} in {zone}."

    return f"{len(rows)} measurements returned across multiple metrics."


# ===================== HIGH LEVEL HANDLERS =====================

def answer_sensor_query(user_query: str) -> Dict[str, Any]:
    intent = parse_intent(user_query)
    flux = build_flux_from_intent(intent)

    if not flux:
        flux = llm_flux_fallback(user_query)

    if not flux:
        return {
            "mode": "sensor",
            "reply": "I could not construct a valid Flux query for that request.",
            "flux": None,
            "rows": []
        }

    logging.info(f"Flux: {flux}")

    try:
        rows = query_influx(flux)
    except Exception as e:
        return {
            "mode": "sensor",
            "reply": f"InfluxDB query failed: {e}",
            "flux": flux,
            "rows": []
        }

    reply = simple_summary(intent, rows)
    return {
        "mode": "sensor",
        "reply": reply,
        "flux": flux,
        "rows": rows
    }


def answer_general_chat(user_query: str, history: List[Dict[str, str]]) -> Dict[str, Any]:
    system_prompt = (
        "You are ODIN, a city-scale digital-twin assistant. "
        "You speak clearly, briefly, and concretely. "
        "You can discuss sustainability, mobility, data, sensors, "
        "and general questions. Avoid hallucinating detailed data; "
        "if you do not know exact numbers, speak qualitatively."
    )

    # Map incoming history to OpenAI-style roles
    msgs: List[Dict[str, str]] = []
    for h in history:
        role = h.get("role", "user")
        if role not in ("user", "assistant", "system"):
            role = "user"
        msgs.append({"role": role, "content": h.get("content", "")})
    msgs.append({"role": "user", "content": user_query})

    try:
        answer = call_llm(system_prompt, msgs)
    except Exception as e:
        answer = f"There was an error talking to the language model: {e}"

    return {
        "mode": "chat",
        "reply": answer,
        "flux": None,
        "rows": []
    }


# ===================== HTTP API =====================

@app.route("/odin/chat", methods=["POST"])
def odin_chat():
    """
    JSON body:
      {
        "message": "string",             # required
        "history": [ {role, content} ]   # optional
      }
    """
    try:
        data = request.get_json(force=True)
    except Exception:
        return jsonify({"ok": False, "error": "invalid_json"}), 400

    message = (data or {}).get("message", "")
    history = (data or {}).get("history", []) or []

    if not isinstance(message, str) or not message.strip():
        return jsonify({"ok": False, "error": "empty_message"}), 400

    message = message.strip()
    logging.info(f"[ODIN] incoming: {message}")

    try:
        if is_sensor_query(message):
            result = answer_sensor_query(message)
        else:
            result = answer_general_chat(message, history)
    except Exception as e:
        logging.exception("Unhandled error in ODIN chat")
        return jsonify({
            "ok": False,
            "error": str(e)
        }), 500

    return jsonify({
        "ok": True,
        "timestamp": utc_now_iso(),
        **result
    })


# ===================== CLI TEST =====================

if __name__ == "__main__":
    # Simple local test
    print("Starting ODIN chat on http://0.0.0.0:5100/odin/chat")
    app.run(host="0.0.0.0", port=5100, debug=True)
