from flask import Flask, request, jsonify
import mysql.connector
import random, subprocess, time, uuid, os

app = Flask(__name__)

MYSQL_DB   = os.getenv("MYSQL_DB", "sakila")
MYSQL_USER = os.getenv("MYSQL_USER", "app")
MYSQL_PASS = os.getenv("MYSQL_PASS", "AppPass123!")

MANAGER_HOST = os.environ["MANAGER_HOST"]          
WORKER1_HOST = os.environ["WORKER1_HOST"]          
WORKER2_HOST = os.environ["WORKER2_HOST"]          

MANAGER = {"host": MANAGER_HOST, "user": MYSQL_USER, "password": MYSQL_PASS}
WORKERS = [
    {"host": WORKER1_HOST, "user": MYSQL_USER, "password": MYSQL_PASS},
    {"host": WORKER2_HOST, "user": MYSQL_USER, "password": MYSQL_PASS},
]

def is_read(sql: str) -> bool:
    s = (sql or "").strip().lower()
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
        database=MYSQL_DB,
        autocommit=True
    )
    cur = conn.cursor()
    cur.execute(sql)
    rows = cur.fetchall() if cur.with_rows else []
    cur.close()
    conn.close()
    return {"rows": rows}

@app.get("/health")
def health():
    return "ok"

@app.post("/query")
def query():
    body = request.get_json(force=True)
    sql = body.get("sql", "")
    mode = body.get("mode", "direct")
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