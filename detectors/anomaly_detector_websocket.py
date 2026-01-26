#!/usr/bin/env python3
"""
Anomaly Detector WebSocket Server
AUTHOR: Daniel Wondyifraw DataTwinLabs.nl

Real-time anomaly detection server with WebSocket support for live alerts.
"""

import time
import json

from flask import Flask, request, jsonify
from flask_socketio import SocketIO, emit

app = Flask(__name__)
app.config["SECRET_KEY"] = "anything-you-want"

# Use threading backend, avoid eventlet drama
socketio = SocketIO(
    app,
    cors_allowed_origins="*",
    ping_timeout=60,
    ping_interval=25,
    async_mode="threading",
)

# ---------------------------------------------------
# Broadcast helper
# ---------------------------------------------------
last_emit_time = 0

def push_anomaly(event):
    global last_emit_time
    now = time.time()
    if now - last_emit_time < 2:
        return  # ignore high frequency junk
    last_emit_time = now
    socketio.emit("anomaly", event)


# ---------------------------------------------------
# HTTP endpoint used by kafka_anomaly_bridge.py
# ---------------------------------------------------
@app.route("/api/anomaly", methods=["POST"])
def api_anomaly():
    try:
        data = request.get_json(force=True)
    except Exception:
        return jsonify({"ok": False, "error": "invalid_json"}), 400

    if "sensor_id" not in data or "anomaly" not in data:
        return jsonify({"ok": False, "error": "missing_fields"}), 400

    if data["anomaly"] != 1:
        return jsonify({"ok": True, "skipped": "not_anomaly"}), 200

    push_anomaly(data)
    return jsonify({"ok": True}), 200


# ---------------------------------------------------
# WebSocket events
# ---------------------------------------------------
@socketio.on("connect")
def ws_connect():
    print("[WS] client connected")
    emit("status", {"message": "connected"})

@socketio.on("disconnect")
def ws_disconnect():
    print("[WS] client disconnected")


# ---------------------------------------------------
# MAIN
# ---------------------------------------------------
if __name__ == "__main__":
    print("Starting anomaly WebSocket server on :5000 ...")
    # debug=True so you see errors if it dies
    socketio.run(app, host="0.0.0.0", port=5000, debug=True)
