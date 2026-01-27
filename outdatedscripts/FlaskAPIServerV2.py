#!/usr/bin/env python3
import os, time, logging
from functools import wraps
from flask import Flask, request, jsonify
from flask_cors import CORS
from werkzeug.exceptions import BadRequest
import ExplainerFinal as LLMResponse

# ---------- Config ----------
ALLOWED_ORIGINS = os.getenv("ALLOWED_ORIGINS", "localhost").split(",")
API_TOKEN = os.getenv("khnsadflku732497182913791283a8s9q8we37q")  # optional simple auth
REQUEST_TIMEOUT_SEC = int(os.getenv("REQUEST_TIMEOUT_SEC", "15"))

app = Flask(__name__)
CORS(app, origins=ALLOWED_ORIGINS)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

def require_token(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        if API_TOKEN:
            token = request.headers.get("X-API-Key")
            if token != API_TOKEN:
                return jsonify({"error": "unauthorized"}), 401
        return f(*args, **kwargs)
    return wrapper

def json_error(message, code=400):
    return jsonify({"error": message}), code

# ---------- Routes ----------
@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"}), 200

@app.route("/query", methods=["POST"])
@require_token
def handle_query():
    t0 = time.time()
    if not request.is_json:
        return json_error("Content-Type must be application/json", 415)
    try:
        payload = request.get_json(force=True, silent=False)
    except BadRequest:
        return json_error("Invalid JSON", 400)

    user_input = (payload.get("query") or "").strip()
    if not user_input:
        return json_error("Missing field: 'query'", 400)

    app.logger.info("query=%r", user_input)

    try:
        # Your function should return a STRUCTURED dict, not just a string
        # contract: {"answer": str, "facts": {...}, "parcel": {...}, "model": str}
        result = LLMResponse.handle_query(user_input, timeout=REQUEST_TIMEOUT_SEC)

        if not isinstance(result, dict) or "answer" not in result:
            # Backward-compat shim for your old string-returning code
            result = {"answer": str(result), "facts": {}, "parcel": {}, "model": "unknown"}

        result.setdefault("latency_ms", int((time.time() - t0) * 1000))
        return jsonify(result), 200

    except Exception as e:
        app.logger.exception("query failed")
        return json_error(f"internal_error: {type(e).__name__}", 500)

# ---------- Entrypoint ----------
if __name__ == "__main__":
    # Local dev only. In prod run: gunicorn -w 2 -k gevent -b 0.0.0.0:5050 app:app
    app.run(host="0.0.0.0", port=5050, debug=False)
