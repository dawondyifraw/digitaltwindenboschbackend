#!/usr/bin/env python3

import requests
import re
import json
from influxdb_client import InfluxDBClient
from typing import List, Dict

# ---------- Config ----------
OLLAMA_URL = "http://localhost:11434/api/generate"
INFLUX_URL = "http://localhost:8086"
INFLUX_TOKEN = "vQZH5VT3VRUrDBdX4oyDO5BV7kWN4NvRvUJUSvkOGEz-cL3huzmpkBo5ywMVBioDXNQ0UfHc3afinUpxFnLmA=="
INFLUX_ORG = "DenBosch"
KADASTER_API_URL = "https://api.pdok.nl/kadaster/brk-percelen/v1/percelen"


#-------hardcodequery-------------------------------

import requests
import json
from influxdb_client import InfluxDBClient
from typing import List, Dict

# ---------- Config ----------
OLLAMA_URL = "http://localhost:11434/api/generate"
INFLUX_URL = "http://localhost:8086"
INFLUX_TOKEN = "vQZH5VT3VRUrDBdX4oyDO5BV7kWN4NvRvUJUSvkOGEz-cL3huzmpkBo5ywMVBioDXNQ0UfHc3afinUpxFnLmA=="
INFLUX_ORG = "DenBosch"
KADASTER_API_URL = "https://api.pdok.nl/kadaster/brk-percelen/v1/percelen"

# ---------- Hardcoded Flux Query ----------
def get_flux_query() -> str:
    """Return a hardcoded Flux query for InfluxDB."""
    return """
    from(bucket: "sensordb_influx")
      |> range(start: -1d)  // Last 24 hours
      |> filter(fn: (r) => r["_measurement"] == "kafka_consumer")
      |> filter(fn: (r) => r["_field"] == "co2" or r["_field"] == "sound_level" or r["_field"] == "longitude" or r["_field"] == "latitude")
      |> sort(columns: ["_time"], desc: true)
      |> limit(n: 10)
    """

# ---------- Kadaster API Integration ----------
def get_kadaster_details(lat: float, lon: float) -> Dict:
    """Get land parcel data from Kadaster BRK API"""
    params = {"lat": lat, "lon": lon}
    try:
        response = requests.get(
            KADASTER_API_URL,
            params=params,
            headers={"Accept": "application/json"}
        )
        response.raise_for_status()
        data = response.json()

        if not data.get("features"):
            return {}

        # Extract key parcel details from first feature
        properties = data["features"][0].get("properties", {})
        geometry = data["features"][0].get("geometry", {})

        return {
            "perceel_id": properties.get("perceelnummer"),
            "area_m2": properties.get("oppervlakte"),
            "land_use": properties.get("gebruiksdoel"),
            "ownership": properties.get("zakelijk_recht_naam"),
            "geometry_type": geometry.get("type"),
            "coordinates": geometry.get("coordinates")
        }
    except requests.exceptions.RequestException as e:
        print(f"Error fetching Kadaster data: {str(e)}")
        return {}

# ---------- AI Explanation Based on Data ----------
def explain_results(user_query: str, influx_data: List[Dict], kadaster_data: Dict) -> str:
    """Generate natural language response combining InfluxDB and Kadaster data"""
    prompt = f"""
    User question: "{user_query}"
    
    Environmental sensor data:
    {json.dumps(influx_data, indent=2)}

    Land parcel details:
    {json.dumps(kadaster_data, indent=2)}

    Provide a **concise** answer:
    1. Use sensor data to **answer** the question.
    2. Include **relevant** land parcel metadata.
    3. Highlight **notable** relationships.
    """

    try:
        response = requests.post(
            OLLAMA_URL,
            json={"model": "llama2", "prompt": prompt, "stream": False}
        )
        response.raise_for_status()
        response_data = response.json()
        return response_data.get("response", "").strip()
    except requests.exceptions.RequestException as e:
        return f"Error generating response: {str(e)}"

# ---------- Main Workflow ----------
def handle_query(user_query: str) -> str:
    """Handles the full pipeline: Fetch InfluxDB data → Fetch Kadaster Data → AI Explanation"""
    client = None  # Initialize client to None
    try:
        # Use the hardcoded Flux query
        flux_query = get_flux_query()
        print(f"✅ Final Flux Query:\n{flux_query}")  # Debugging

        # Query InfluxDB
        client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
        result = client.query_api().query(flux_query)

        #print(f"result are {result}")

        # Extract coordinates from query results
        records = []
        for table in result:
            for rec in table.records:
                lat = rec.values.get("latitude") or rec.values.get("_value")  # Adjusted extraction logic
                lon = rec.values.get("longitude") or rec.values.get("_value")

                print(f"lat and lon are {lat} and {lon}")

                if lat is not None and lon is not None:
                    records.append({
                        "time": rec.get_time().isoformat(),
                        "lat": lat,
                        "lon": lon,
                        "value": rec.values.get("_value")
                    })

        if not records:
            return "No sensor data found matching the query."

        # Get parcel data for most relevant coordinate
        primary_coord = records[0]
        parcel_data = get_kadaster_details(primary_coord["lat"], primary_coord["lon"])

        # Generate final answer
        return explain_results(user_query, records[:3], parcel_data)

    except Exception as e:
        return f"❌ Error querying InfluxDB: {str(e)}"
    finally:
        if client is not None:  # Only close client if it was successfully initialized
            client.close()

# ---------- Example Usage ----------
if __name__ == "__main__":
    query = "Which industrial area had the highest CO2 levels yesterday?"
    answer = handle_query(query)
    print(answer)
    


 