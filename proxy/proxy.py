from flask import Flask, request, jsonify
import mysql.connector
import random
import subprocess
import time
import uuid

app = Flask(__name__)

MANAGER = {"host": "172.31.26.162", "user": "app", "password": "AppPass123!"}
WORKERS = [
    {"host": "172.31.30.100", "user": "app", "password": "AppPass123!"},
    {"host": "172.31.20.128", "user": "app", "password": "AppPass123!"},
]

def is_read(sql: str) -> bool:
    s = sql.strip().lower()
    return s.startswith("select") or s.startswith("show") or s.startswith("describe")

def ping_ms(host: str) -> float:
    start = time.time()
    subprocess.run(["ping", "-c", "1", "-W", "1", host], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    return (time.time() - start) * 1000.0

def choose_worker(mode: str):
    if mode == "random":
        return random.choice(WORKERS)
    if mode == "ping":
        scored = [(ping_ms(w["host"]), w) for w in WORKERS]
        scored.sort(key=lambda x: x[0])
        return scored[0][1]
    return None

def run_mysql(target, sql):
    conn = mysql.connector.connect(
        host=target["host"],
        user=target["user"],
        password=target["password"],
        database="sakila",
        autocommit=True
    )
    cur = conn.cursor()
    cur.execute(sql)
    if cur.with_rows:
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return {"rows": rows}
    cur.close()
    conn.close()
    return {"rows": []}

@app.get("/health")
def health():
    return "ok"

@app.post("/query")
def query():
    body = request.get_json(force=True)
    sql = body.get("sql", "")
    mode = body.get("mode", "direct")  # direct | random | ping
    req_id = body.get("request_id") or str(uuid.uuid4())

    read = is_read(sql)

    if mode == "direct":
        target = MANAGER
        target_name = "manager"
    else:
        if read:
            target = choose_worker(mode)
            target_name = "worker@" + target["host"]
        else:
            target = MANAGER
            target_name = "manager"

    result = run_mysql(target, sql)

    print(f"[{req_id}] mode={mode} type={'READ' if read else 'WRITE'} target={target_name}")

    return jsonify({
        "request_id": req_id,
        "mode": mode,
        "type": "READ" if read else "WRITE",
        "target": target_name,
        "result": result
    })

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
