#!/usr/bin/env python3
"""
Smart City Dashboard API

Serves aggregated data from InfluxDB for the frontend dashboard:
- /api/influx/dashboard  -> charts + latestSensors + basic metadata

Focus:
- Real-time sensor data from InfluxDB
- Aggregated metric averages for charts
- Static placeholders for demographics / housing / emissions (can be replaced later)
"""

import logging
from datetime import datetime, timezone
from typing import List, Dict, Any

from flask import Flask, jsonify
from flask_cors import CORS
from influxdb_client import InfluxDBClient

# ===================== FLASK APP / CORS =====================

app = Flask(__name__)
CORS(app, resources={
    r"/api/*": {
        "origins": [
            "http://localhost:8000",
            "http://127.0.0.1:8000",
            "https://datatwinlabs.nl",
            "http://localhost:3000",
            "*"
        ]
    }
})

# ===================== CONFIG =====================

class Config:
    # InfluxDB connection
    INFLUX_URL = "http://localhost:8086"
    INFLUX_TOKEN = "vQZH5VT3VRUrDBdX4oyDO5BV7kWN4NvRvUJUSvkOGEz-cL3huzmpkBo5ywMVBioDXNQ0UfHc3afinUpxFnLmA=="  # put your real token here
    BUCKET = "sensors_db"
    INFLUX_ORG = "DenBosch"
    MEASUREMENT = "environment"

    # Time window for latest sensor snapshot (minutes)
    # Keep large while debugging
    SENSOR_WINDOW_MINUTES = 720

    # Metrics to pull – MUST match _field names in Influx
    # 'id' is what the frontend expects in `row.metric`
    METRICS: Dict[str, Dict[str, str]] = {
        "co2_ppm": {
            "id": "co2_ppm",
            "label": "CO₂",
            "unit": "ppm",
        },
        "pm25_ugm3": {
            "id": "pm25_ugm3",
            "label": "PM2.5",
            "unit": "µg/m³",
        },
        "no2_ppb": {
            "id": "no2_ppb",
            "label": "NO₂",
            "unit": "ppb",
        },
        "noise_db": {
            "id": "noise_db",
            "label": "Noise",
            "unit": "dB",
        },
    }


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(name)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger("DASHBOARD_API")


# ===================== INFLUX HELPERS =====================

def get_influx_client() -> InfluxDBClient:
    """Create a new InfluxDB client instance."""
    return InfluxDBClient(
        url=Config.INFLUX_URL,
        token=Config.INFLUX_TOKEN,
        org=Config.INFLUX_ORG
    )


def query_latest_sensors() -> List[Dict[str, Any]]:
    """
    Query InfluxDB for latest sensor readings for each metric and sensor.

    Returns list of:
    {
        "sensor_id": str,
        "location": str (zone),
        "metric": str,        # canonical id e.g. 'co2_ppm'
        "metric_label": str,  # nice label e.g. 'CO₂'
        "value": float,
        "unit": str,
        "time": ISO8601 str
    }
    """
    metric_fields = list(Config.METRICS.keys())
    if not metric_fields:
        logger.warning("No metrics configured in Config.METRICS")
        return []

    field_filter = " or ".join([f'r._field == "{m}"' for m in metric_fields])

    flux = f"""
    from(bucket: "{Config.BUCKET}")
      |> range(start: -{Config.SENSOR_WINDOW_MINUTES}m)
      |> filter(fn: (r) => r._measurement == "{Config.MEASUREMENT}")
      |> filter(fn: (r) => {field_filter})
      |> group(columns: ["sensor_id", "zone", "_field"])
      |> last()
    """
    flux = " ".join(flux.split())
    logger.info(f"Running Influx query for latest sensors: {flux}")

    client = get_influx_client()
    try:
        tables = client.query_api().query(flux)
        rows: List[Dict[str, Any]] = []

        for table in tables:
            for record in table.records:
                field = record.get_field()
                metric_cfg = Config.METRICS.get(field)
                if not metric_cfg:
                    # Unknown field, ignore
                    continue

                raw_value = record.get_value()
                try:
                    value = float(raw_value)
                except Exception:
                    value = raw_value

                t = record.get_time()
                sensor_id = record.values.get("sensor_id")
                zone = record.values.get("zone")

                rows.append({
                    "sensor_id": sensor_id,
                    "location": zone,
                    # IMPORTANT: metric = canonical id used by frontend (e.g. 'co2_ppm')
                    "metric": metric_cfg["id"],
                    "metric_label": metric_cfg["label"],
                    "value": value,
                    "unit": metric_cfg["unit"],
                    "time": t.isoformat() if t else None,
                })

        logger.info(f"Latest sensor records: {len(rows)}")
        return rows

    except Exception as e:
        logger.error(f"Error querying latest sensors: {e}", exc_info=True)
        return []
    finally:
        client.close()


def build_sensor_summary(latest_sensors: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Aggregate latest sensors into simple averages per metric
    for the 'sensorChart'.

    Returns:
    {
        "labels": [... nice labels ...],
        "values": [... avg value per metric ...]
    }
    """
    if not latest_sensors:
        return {"labels": [], "values": []}

    agg: Dict[str, Dict[str, Any]] = {}
    for r in latest_sensors:
        metric_id = r.get("metric")
        if not metric_id:
            continue

        try:
            v = float(r.get("value"))
        except Exception:
            continue

        if metric_id not in agg:
            agg[metric_id] = {"sum": 0.0, "count": 0}

        agg[metric_id]["sum"] += v
        agg[metric_id]["count"] += 1

    labels: List[str] = []
    values: List[float] = []

    for metric_id, stats in agg.items():
        cfg = Config.METRICS.get(metric_id, {"label": metric_id})
        labels.append(cfg["label"])
        if stats["count"] > 0:
            values.append(stats["sum"] / stats["count"])
        else:
            values.append(0.0)

    return {"labels": labels, "values": values}


# ===================== STATIC / PLACEHOLDER BLOCKS =====================

def build_static_blocks() -> Dict[str, Any]:
    """
    Static data for non-sensor charts.
    Replace with real queries later if needed.
    """
    return {
        "populationGrowth": {
            "labels": ['2018', '2019', '2020', '2021', '2022', '2023', '2024'],
            "values": [85000, 87000, 89000, 92000, 95000, 98000, 101000],
        },
        "ageDemographics": {
            "labels": ['0-18', '19-35', '36-50', '51-65', '65+'],
            "values": [22, 35, 25, 12, 6],
        },
        "housing": {
            "labels": ['Apartments', 'Single Family', 'Townhouses'],
            "values": [45, 35, 20],
        },
        "pollution": {
            "labels": ['Transport', 'Industry', 'Residential', 'Other'],
            "values": [40, 25, 20, 15],
        },
        "emissions": {
            "labels": ['CO2', 'NOx', 'SO2', 'PM2.5', 'VOC'],
            "values": [65, 45, 30, 55, 25],
        },
        "energyUsage": {
            "labels": ['00:00', '06:00', '12:00', '18:00'],
            "values": [120, 90, 150, 180],
        },
        "trafficDensity": {
            "labels": ['6AM', '9AM', '12PM', '3PM', '6PM', '9PM'],
            "values": [1200, 3500, 2800, 3200, 4100, 1800],
        }
    }


# ===================== ROUTES =====================

@app.route("/health", methods=["GET"])
def health():
    return jsonify({
        "status": "ok",
        "service": "SmartCityDashboardAPI",
        "timestamp": datetime.now(timezone.utc).isoformat()
    })


@app.route("/api/influx/dashboard", methods=["GET"])
def api_influx_dashboard():
    """
    Main endpoint consumed by the dashboard frontend.

    Returns:
    - latestSensors: list of latest sensor readings
    - sensorData: aggregated averages per metric
    - populationGrowth, ageDemographics, housing, pollution, emissions,
      energyUsage, trafficDensity
    """
    try:
        latest_sensors = query_latest_sensors()
        sensor_summary = build_sensor_summary(latest_sensors)
        static_blocks = build_static_blocks()

        payload: Dict[str, Any] = {
            "ok": True,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "windowMinutes": Config.SENSOR_WINDOW_MINUTES,
            "latestSensors": latest_sensors,
            "sensorData": sensor_summary,
            **static_blocks,
        }

        return jsonify(payload), 200

    except Exception as e:
        logger.exception("Error in /api/influx/dashboard")
        return jsonify({"ok": False, "error": str(e)}), 500


# ===================== MAIN =====================

if __name__ == "__main__":
    logger.info("Starting Smart City Dashboard API on http://localhost:5001")
    app.run(host="0.0.0.0", port=5001, debug=False)
