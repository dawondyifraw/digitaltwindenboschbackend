#!/usr/bin/env python3

import requests , re
import json
from influxdb_client import InfluxDBClient
from typing import List, Dict
from typing import Dict, List, Any,Optional
import nominatim_api
import logging


# =============== CONFIG ===============
HYPERBOLIC_URL = "https://api.hyperbolic.xyz/v1/chat/completions"
#HYPERBOLIC_URL = "http://localhost:11434/v1/chat/completions"  # Update the port/path as needed.

LOCAL_URL = "http://ollama:11434"

HYPERBOLIC_API_KEY = ""  # Replace with your actual API key
INFLUX_URL = "http://localhost:8086"
INFLUX_TOKEN = "vQZH5VT3VRUrDBdX4oyDO5BV7kWN4NvRvUJUSvkOGEz-cL3huzmpkBo5ywMVBioDXNQ0UfHc3afinUpxFnLmA==" ##replace
INFLUX_ORG = "DenBosch"
KADASTER_API_URL = "https://api.pdok.nl/kadaster/brk-percelen/v1/percelen"
LOCATIESERVER_URL = "https://geodata.nationaalgeoregister.nl/locatieserver/v3/search"
BUCKET_NAME = "sensordb_influx"
BUCKET_NAME = "sensordb_influx"
MEASUREMENT_NAME = "kafka_consumer"
SENSOR_FIELDS = ["co2", "sound_level", "no2"]
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
# ---------------- Helper Functions ----------------

def sanitize_flux_query(q: str) -> str:
    q = q.strip()
    q = q.replace("’", "'").replace("‘", "'")
    q = q.replace("“", '"').replace("”", '"')
    q = q.replace("\u00A0", " ")
    q = " ".join(q.splitlines())
    q = re.sub(r"from\(bucket:\s*'([^']+)'\)", r'from(bucket: "\1")', q)
    return q

def force_ascii_single_line_flux(flux: str) -> str:
    flux = flux.strip()
    flux = " ".join(flux.splitlines())
    flux = flux.replace("’", "'").replace("‘", "'")
    flux = flux.replace("“", '"').replace("”", '"')
    flux = flux.replace("\u00A0", " ")
    flux = re.sub(r"from\(bucket:\s*'([^']+)'\)", r'from(bucket: "\1")', flux)
    flux = "".join(ch for ch in flux if ord(ch) < 128)
    return flux

def is_off_topic(user_query: str) -> bool:
    uq = user_query.lower()
    relevant_terms = ["co2", "sound_level", "no2", "pm25", "highest", "lowest", "average", "mean", "top", "bottom", "latest"]
    return not any(term in uq for term in relevant_terms)

def extract_query_parameters(user_query: str) -> Dict[str, Any]:
    params = {}
    uq = user_query.lower()
    if "last hour" in uq:
        params["start"] = "-1h"
    elif "last day" in uq:
        params["start"] = "-1d"
    elif "last two days" in uq:
        params["start"] = "-2d"
    else:
        params["start"] = "-2d"
    if "average" in uq or "mean" in uq:
        params["aggregator"] = "mean"
    else:
        params["aggregator"] = None
    if "highest" in uq or "max" in uq or "top" in uq:
        params["sort"] = "desc"
        params["limit"] = 1
    elif "lowest" in uq or "min" in uq or "bottom" in uq:
        params["sort"] = "asc"
        params["limit"] = 1
    elif "latest" in uq:
        params["sort"] = "desc"
        params["limit"] = 1
    else:
        params["sort"] = None
    return params

# ---------------- LLM: Flux Query Generator ----------------

def generate_flux_query(user_query: str) -> Optional[str]:
    if is_off_topic(user_query):
        return None

    params_extracted = extract_query_parameters(user_query)
    time_range = params_extracted.get("start", "-2d")
    aggregator = params_extracted.get("aggregator")
    sort = params_extracted.get("sort")
    limit = params_extracted.get("limit")

    dynamic_instructions = f"\nAdditional parameters detected:\n- Time range: {time_range}\n"
    if aggregator:
        dynamic_instructions += f"- Use aggregator: {aggregator}\n"
    if sort:
        dynamic_instructions += f"- Sort order: {sort} with limit: {limit}\n"

    system_instructions = (
        "You are an expert data engineer specialized in writing InfluxDB Flux queries.\n"
        f"We have the following data schema:\n"
        f"- Bucket: {BUCKET_NAME}\n"
        f"- Measurement: {MEASUREMENT_NAME}\n"
        f"- Numeric fields: {', '.join(SENSOR_FIELDS)}\n"
        f"- If no time range is specified, default to the last two days.\n"
        f"Dynamic query adjustments:{dynamic_instructions}\n"
        "Goal:\n"
        f"1) Begin with: from(bucket: '{BUCKET_NAME}') |> range(start: {time_range}).\n"
        "2) Filter on _measurement == \"kafka_consumer\".\n"
    )
    
    if aggregator:
        system_instructions += "3) Use aggregateWindow(every: 1h, fn: mean, createEmpty: false) to compute the aggregator.\n"
    if sort == "desc":
        system_instructions += "4) After pivoting, use top(n: 1, columns: [\"co2\"]) to get the highest value.\n"
    elif sort == "asc":
        system_instructions += "4) After pivoting, use bottom(n: 1, columns: [\"co2\"]) to get the lowest value.\n"
    elif sort == "desc" and "latest" in user_query.lower():
        system_instructions += "4) After pivoting, sort by _time in descending order and limit to 1 to get the latest record.\n"
    else:
        system_instructions += "4) Do not apply explicit top() or bottom() functions.\n"
    
    system_instructions += (
        "5) Pivot the result so that _time, co2, sound_level, no2, latitude, and longitude become columns.\n"
        "6) Keep (_time plus all sensor fields) in the final table.\n"
        "7) Return ONLY the Flux query as a single-line plain text string (no code fences, no extra explanation).\n"
        "8) Even if the user references only one field (e.g. co2), include all sensor fields if available.\n"
    )
    
    messages = [
        {"role": "system", "content": system_instructions},
        {"role": "user", "content": user_query}
    ]
    headers_llm = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {HYPERBOLIC_API_KEY}"
    }
    payload = {
        "messages": messages,
        "model": "deepseek-ai/DeepSeek-V3",
        "max_tokens": 512,
        "temperature": 0.0,
        "top_p": 1.0
    }
    
    try:
        response = requests.post(HYPERBOLIC_URL, json=payload, headers=headers_llm)
        response.raise_for_status()
        response_data = response.json()
        flux_query = response_data.get("choices", [{}])[0].get("message", {}).get("content", "")
        flux_query = flux_query.replace("```flux", "").replace("```", "").strip()
        flux_query = sanitize_flux_query(flux_query)
        flux_query = flux_query.replace("'", '"')  # force double quotes
        return flux_query
    except requests.RequestException as e:
        logging.error(f"Error generating Flux query: {e}")
        fallback = (
            'from(bucket: "sensordb_influx") '
            '|> range(start: -1d) '
            '|> filter(fn: (r) => r._measurement == "kafka_consumer") '
            '|> filter(fn: (r) => r._field == "co2") '
            '|> aggregateWindow(every: 1h, fn: mean, createEmpty: false) '
            '|> yield(name: "mean")'
        )
        return force_ascii_single_line_flux(fallback)

# ---------------- Nominatim Reverse Geocoding ----------------

def get_nominatim_address(lat: float, lon: float) -> Dict:
    """
    Uses the Nominatim API to perform a reverse geocode lookup based on
    the given latitude and longitude.
    Returns the JSON response as a dictionary if successful, or an empty dict.
    """
    url = "https://nominatim.openstreetmap.org/reverse"
    params = {
        "format": "json",
        "lat": lat,
        "lon": lon,
        "addressdetails": 1
    }
    headers = {
        "User-Agent": "DigitalTwinDebBosch/1.0 (daniel@edigitaltwindenbosch.com"  # Replace with your app details
    }
    try:
        response = requests.get(url, params=params, headers=headers, timeout=10)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        logging.error(f"Error fetching Nominatim address: {e}")
        return {}

# ---------------- LLM: Explanation ----------------

def explain_results(user_query: str, influx_data: List[Dict], nominatim_data: Dict) -> str:
    """
    Calls an LLM to generate a concise explanation that references the user query,
    the sensor data from InfluxDB, and uses the reverse geocoded Nominatim data
    to resolve the coordinates into a human-readable place name.
    """
    place_hint = ""
    if nominatim_data:
        # Use the 'display_name' from Nominatim as the short place name.
        place = nominatim_data.get("display_name")
        if place:
            place_hint = f" The coordinates correspond to: {place}."
    
    prompt = f"""
User question: "{user_query}"

Sensor data (InfluxDB):
{json.dumps(influx_data, indent=2)}

Nominatim reverse geocode info:
{json.dumps(nominatim_data, indent=2)}

Produce a concise answer that:
1) Summarizes how the sensor data addresses the question.
2) Resolves the provided latitude and longitude into a human-readable place name.
3) Incorporates the resolved place name in your explanation.
{place_hint}
"""
    headers_llm = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {HYPERBOLIC_API_KEY}"
    }
    payload = {
        "messages": [{"role": "user", "content": prompt}],
        "model": "deepseek-ai/DeepSeek-V3",
        "max_tokens": 512,
        "temperature": 0.1,
        "top_p": 0.9
    }
    try:
        response = requests.post(HYPERBOLIC_URL, headers=headers_llm, json=payload)
        response.raise_for_status()
        response_data = response.json()
        return response_data.get("choices", [{}])[0].get("message", {}).get("content", "").strip()
    except requests.RequestException as e:
        return f"Error generating final explanation: {e}"

# ---------------- Main Workflow ----------------

def handle_query(user_query: str) -> str:
    """
    1) If the query is off-topic, return a simple answer.
    2) Otherwise, generate a Flux query via the LLM (with dynamic adjustments).
    3) Sanitize and force the query into a single ASCII line.
    4) Query InfluxDB.
    5) Parse the sensor records.
    6) Use Nominatim to reverse geocode the coordinates into address data.
    7) Generate a final explanation via the LLM that includes the resolved place name.
    """
    if is_off_topic(user_query):
        return f"You asked: '{user_query}'. It doesn't seem related to our sensor data. How can I help?"
    
    flux_query = generate_flux_query(user_query)
    if not flux_query:
        return "No valid Flux query generated. Your query may be off-topic or ambiguous."
    
    logging.info(f"Generated Flux Query: {flux_query}")
    logging.debug(f"Flux Query BEFORE forcing: {repr(flux_query)}")
    flux_query = force_ascii_single_line_flux(flux_query)
    logging.debug(f"Flux Query AFTER forcing: {repr(flux_query)}")
    
    client = None
    try:
        client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
        tables = client.query_api().query(query=flux_query)
        
        records = []
        for table in tables:
            for rec in table.records:
                time_val = rec.get_time() if hasattr(rec, "get_time") else rec.values.get("_time")
                row = {
                    "time": time_val.isoformat() if time_val else None,
                    "co2": rec.values.get("co2"),
                    "sound_level": rec.values.get("sound_level"),
                    "no2": rec.values.get("no2"),
                    "pm25": rec.values.get("pm25"),
                    "latitude": rec.values.get("latitude"),
                    "longitude": rec.values.get("longitude")
                }
                records.append(row)
        
        if not records:
            return "No sensor data returned by the query."
        
        # Use the first record's coordinates for reverse geocoding via Nominatim
        primary_lat = records[0].get("latitude")
        primary_lon = records[0].get("longitude")
        nominatim_data = {}
        if primary_lat is not None and primary_lon is not None:
            nominatim_data = get_nominatim_address(primary_lat, primary_lon)
        
        return explain_results(user_query, records[:5], nominatim_data)
    
    except Exception as e:
        logging.error(f"Error in handle_query: {e}")
        return f"❌ Error in handle_query: {e}"
    finally:
        if client is not None:
            client.close()

# ---------------- Example Usage ----------------

if __name__ == "__main__":
    # Example query: "What is the average CO2 level in the last day?"
    user_q = "What is the average CO2 level in the last day?"
    print("USER QUERY:", user_q)
    answer = handle_query(user_q)
    print("Final Answer:\n", answer)