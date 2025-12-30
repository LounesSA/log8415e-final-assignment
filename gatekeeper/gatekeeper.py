from flask import Flask, request, jsonify
import requests
import uuid

app = Flask(__name__)

PROXY_URL = "http://172.31.30.140:5000/query"
API_KEY = "MY_SECRET_KEY_123"

BLOCKLIST = ["drop ", "truncate ", "shutdown ", "grant ", "revoke "]

def allowed(sql: str) -> bool:
    s = sql.strip().lower()
    for bad in BLOCKLIST:
        if bad in s:
            return False
    return True

@app.get("/health")
def health():
    return "ok"

@app.post("/query")
def query():
    if request.headers.get("X-API-Key", "") != API_KEY:
        return jsonify({"error": "unauthorized"}), 401

    body = request.get_json(force=True)
    sql = body.get("sql", "")
    mode = body.get("mode", "direct")

    if not allowed(sql):
        return jsonify({"error": "blocked by gatekeeper policy"}), 400

    req_id = body.get("request_id") or str(uuid.uuid4())

    resp = requests.post(PROXY_URL, json={"sql": sql, "mode": mode, "request_id": req_id}, timeout=10)
    return jsonify(resp.json()), resp.status_code

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=80)
