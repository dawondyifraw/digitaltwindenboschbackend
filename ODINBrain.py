#!/usr/bin/env python3
"""
Enhanced ODIN Chat Service with Cesium map actions.

Features:
- /health   -> basic status
- /odin/chat -> chat + intelligent map_actions for Cesium

Map Actions Supported:
- Camera moves to zones/landmarks
- Zoom to specific streets/locations
- Fly to coordinates
- Set camera view with heading/pitch/roll
"""

import json
import logging
import time
import uuid
import re
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional, Tuple

import requests
from flask import Flask, request, jsonify
from flask_cors import CORS

# ===================== CONFIG =====================

class Config:
    # Hyperbolic / LLM
    HYPERBOLIC_URL = "https://api.hyperbolic.xyz/v1/chat/completions"
    HYPERBOLIC_API_KEY = "REPLACE_WITH_YOUR_HYPERBOLIC_KEY"
    LLM_MODEL = "deepseek-ai/DeepSeek-V3"

    REQUEST_TIMEOUT = 30
    MAX_HISTORY_LENGTH = 10

# Extended camera positions for 's-Hertogenbosch
LOCATION_CAMERAS: Dict[str, Dict[str, Any]] = {
    # Zones
    "Binnenstad": {"latitude": 51.689, "longitude": 5.303, "height": 800},
    "Station": {"latitude": 51.690, "longitude": 5.293, "height": 700},
    "Zuid": {"latitude": 51.684, "longitude": 5.305, "height": 900},
    
    # Landmarks
    "Sint-Janskathedraal": {"latitude": 51.688, "longitude": 5.305, "height": 400},
    "Binnendieze": {"latitude": 51.687, "longitude": 5.302, "height": 300},
    "Markt": {"latitude": 51.687, "longitude": 5.304, "height": 350},
    "Stadhuis": {"latitude": 51.687, "longitude": 5.304, "height": 350},
    
    # Streets/Areas
    "Hinthamerstraat": {"latitude": 51.688, "longitude": 5.306, "height": 250},
    "Korte Putstraat": {"latitude": 51.687, "longitude": 5.303, "height": 200},
    "Bossche Broek": {"latitude": 51.682, "longitude": 5.298, "height": 600},
    "Paleiskwartier": {"latitude": 51.692, "longitude": 5.295, "height": 600},
}

# Street name mappings (common variations)
STREET_SYNONYMS = {
    "hinthamer": "Hinthamerstraat",
    "putstraat": "Korte Putstraat",
    "korte put": "Korte Putstraat",
    "bossche broek": "Bossche Broek",
    "paleiskwartier": "Paleiskwartier",
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(name)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger("ODIN_ENHANCED")

app = Flask(__name__)
CORS(app)

# ===================== LOCATION PARSING =====================

def extract_location_from_query(user_query: str) -> Optional[Dict[str, Any]]:
    """
    Extract location intent from user query using keyword matching and patterns.
    Returns location info if found, None otherwise.
    """
    uq = user_query.lower().strip()
    
    # Check for exact location matches
    for location_name, camera_config in LOCATION_CAMERAS.items():
        if location_name.lower() in uq:
            return {
                "name": location_name,
                "type": "landmark", 
                "camera_config": camera_config
            }
    
    # Check for street synonyms
    for synonym, canonical_name in STREET_SYNONYMS.items():
        if synonym in uq:
            return {
                "name": canonical_name,
                "type": "street",
                "camera_config": LOCATION_CAMERAS[canonical_name]
            }
    
    # Pattern matching for common location requests
    patterns = [
        (r"(?:show|view|see|look at|zoom to|focus on|go to)\s+(?:the\s+)?(.+?)(?:\s+area|$)", "area"),
        (r"(?:street|straat)\s+(.+?)(?:\s+street|\s+straat|$)", "street"),
        (r"near\s+(.+)", "vicinity"),
        (r"around\s+(.+)", "vicinity"),
        (r"at\s+(.+)", "location"),
    ]
    
    for pattern, loc_type in patterns:
        match = re.search(pattern, uq)
        if match:
            location_mention = match.group(1).strip()
            # Check if this matches any known location
            for location_name in LOCATION_CAMERAS.keys():
                if location_name.lower() in location_mention:
                    return {
                        "name": location_name,
                        "type": loc_type,
                        "camera_config": LOCATION_CAMERAS[location_name]
                    }
    
    return None

def determine_zoom_level(location_type: str, user_query: str) -> float:
    """
    Determine appropriate zoom height based on location type and query context.
    """
    uq = user_query.lower()
    
    # Base heights by location type
    base_heights = {
        "street": 150,
        "landmark": 300, 
        "area": 600,
        "vicinity": 800,
        "location": 500
    }
    
    height = base_heights.get(location_type, 500)
    
    # Adjust based on zoom keywords
    if any(word in uq for word in ["close", "closeup", "detailed", "detail"]):
        height *= 0.5  # Zoom in closer
    elif any(word in uq for word in ["overview", "wide", "broad", "whole area"]):
        height *= 2.0  # Zoom out further
    elif any(word in uq for word in ["zoom in", "closer"]):
        height *= 0.7
    elif any(word in uq for word in ["zoom out", "further"]):
        height *= 1.5
    
    return max(100, height)  # Don't go too low

# ===================== LLM CALL =====================

def call_llm_chat(user_query: str, history: List[Dict[str, str]]) -> str:
    """
    Enhanced chat call that understands map context.
    """
    system_prompt = (
        "You are ODIN, an assistant for an urban digital twin of 's-Hertogenbosch.\n"
        "You help with sensor data, air quality, locations, and urban analytics.\n"
        "IMPORTANT:\n"
        "- The 3D map can be controlled automatically when you mention locations.\n"
        "- When users ask to see/view/zoom to locations, acknowledge this naturally.\n"
        "- For location requests like 'show me X street', confirm and describe briefly.\n"
        "- Keep responses concise but helpful.\n"
        "- Don't over-explain map capabilities, just use them naturally.\n"
    )

    messages: List[Dict[str, str]] = [{"role": "system", "content": system_prompt}]

    for msg in history[-Config.MAX_HISTORY_LENGTH:]:
        messages.append({
            "role": msg.get("role", "user"),
            "content": msg.get("content", "")
        })

    messages.append({"role": "user", "content": user_query})

    payload = {
        "model": Config.LLM_MODEL,
        "max_tokens": 400,
        "temperature": 0.2,
        "messages": messages,
    }

    headers = {
        "Authorization": f"Bearer {Config.HYPERBOLIC_API_KEY}",
        "Content-Type": "application/json",
    }

    try:
        resp = requests.post(
            Config.HYPERBOLIC_URL,
            json=payload,
            headers=headers,
            timeout=Config.REQUEST_TIMEOUT,
        )
        resp.raise_for_status()
        data = resp.json()
        reply = data.get("choices", [{}])[0].get("message", {}).get("content", "").strip()
        if not reply:
            reply = "I could not generate a response. Try asking in a different way."
        return reply
    except Exception as e:
        logger.error(f"LLM error: {e}")
        return "There was an error contacting the language model. Try again later."

# ===================== MAP ACTIONS BUILDER =====================

def build_map_actions(user_query: str) -> List[Dict[str, Any]]:
    """
    Enhanced map logic that handles various location requests intelligently.
    """
    actions: List[Dict[str, Any]] = []
    
    location_info = extract_location_from_query(user_query)
    if not location_info:
        return actions
    
    # Adjust zoom based on context
    adjusted_height = determine_zoom_level(
        location_info["type"], 
        user_query
    )
    
    camera_config = location_info["camera_config"].copy()
    camera_config["height"] = adjusted_height
    
    action = {
        "type": "camera_move",
        "target": {
            "location": location_info["name"],
            "location_type": location_info["type"],
            "latitude": camera_config["latitude"],
            "longitude": camera_config["longitude"], 
            "height": camera_config["height"],
            "duration": 2.0,  # Smooth fly time in seconds
        }
    }
    
    # Add optional camera orientation for certain queries
    uq = user_query.lower()
    if any(word in uq for word in ["north", "south", "east", "west"]):
        if "north" in uq:
            action["target"]["heading"] = 0.0
        elif "south" in uq:
            action["target"]["heading"] = 180.0
        elif "east" in uq:
            action["target"]["heading"] = 90.0
        elif "west" in uq:
            action["target"]["heading"] = 270.0
    
    actions.append(action)
    return actions

# ===================== FLASK ROUTES =====================

@app.route("/health", methods=["GET"])
def health_check():
    return jsonify({
        "status": "healthy",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "service": "ODIN Enhanced Chat with Map Actions",
        "supported_locations": list(LOCATION_CAMERAS.keys()),
    })

@app.route("/odin/chat", methods=["POST"])
def odin_chat():
    start_time = time.time()
    request_id = str(uuid.uuid4())[:8]

    try:
        data = request.get_json(force=True) or {}
    except Exception:
        return jsonify({"ok": False, "error": "invalid_json"}), 400

    message = (data.get("message") or "").strip()
    history = data.get("history", [])

    if not message:
        return jsonify({"ok": False, "error": "empty_message"}), 400

    try:
        logger.info(f"[{request_id}] Incoming message: {message!r}")

        # 1) Get LLM reply
        reply = call_llm_chat(message, history)

        # 2) Determine map actions
        map_actions = build_map_actions(message)
        
        # 3) Determine intent for analytics
        location_info = extract_location_from_query(message)
        if location_info:
            primary_intent = "location_view"
            confidence = 0.95
        else:
            primary_intent = "general_chat" 
            confidence = 0.5

        logger.info(f"[{request_id}] map_actions: {map_actions}")

        result = {
            "mode": "chat",
            "reply": reply,
            "map_actions": map_actions,
            "detected_intents": {
                "primary": primary_intent,
                "location": location_info["name"] if location_info else None,
                "location_type": location_info["type"] if location_info else None,
                "confidence": confidence,
            },
            "flux_query": None,
            "data_points": 0,
            "rows": [],
            "request_id": request_id,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "processing_time": round(time.time() - start_time, 3),
            "ok": True,
        }

        return jsonify(result)

    except Exception as e:
        logger.error(f"[{request_id}] Unhandled error: {e}")
        return jsonify({
            "ok": False,
            "error": "internal_error",
            "request_id": request_id
        }), 500

# ===================== LOCATION INFO ENDPOINT =====================

@app.route("/odin/locations", methods=["GET"])
def get_supported_locations():
    """Endpoint to get all supported locations for the frontend"""
    return jsonify({
        "locations": LOCATION_CAMERAS,
        "street_synonyms": STREET_SYNONYMS,
        "ok": True
    })

# ===================== MAIN =====================

if __name__ == "__main__":
    logger.info("Starting ODIN Enhanced Chat Service with Map Actions")
    logger.info(f"Supported locations: {list(LOCATION_CAMERAS.keys())}")
    app.run(host="0.0.0.0", port=5100, debug=False)