import time
import uuid
import statistics
import requests

GATEKEEPER = "http://16.52.72.103/query"
API_KEY = "MY_SECRET_KEY_123"

N_WRITES = 1000
N_READS = 1000

SLEEP_SEC = 0.005  

def call(sql, mode):
    payload = {"sql": sql, "mode": mode, "request_id": str(uuid.uuid4())}
    headers = {"Content-Type": "application/json", "X-API-Key": API_KEY}

    t0 = time.time()
    r = requests.post(GATEKEEPER, json=payload, headers=headers, timeout=20)
    dt_ms = (time.time() - t0) * 1000.0

    if r.status_code >= 400:
        return dt_ms, {"error_status": r.status_code, "error_text": r.text[:300]}
    try:
        return dt_ms, r.json()
    except Exception:
        return dt_ms, {"error_status": r.status_code, "error_text": r.text[:300]}

def percentile(sorted_arr, p):
    if not sorted_arr:
        return None
    idx = int(p * (len(sorted_arr) - 1))
    return sorted_arr[idx]

def stats(arr):
    if not arr:
        return {"count": 0, "avg_ms": None, "p50_ms": None, "p95_ms": None, "max_ms": None}
    s = sorted(arr)
    return {
        "count": len(s),
        "avg_ms": sum(s) / len(s),
        "p50_ms": percentile(s, 0.50),
        "p95_ms": percentile(s, 0.95),
        "max_ms": max(s),
    }

def run(mode):
    write_lat = []
    read_lat = []
    write_err = 0
    read_err = 0

    # Writes
    for i in range(N_WRITES):
        sql = f'INSERT INTO sbtest.writes(name) VALUES("Bench_{mode}_{i}");'
        dt, resp = call(sql, mode)
        if "error_status" in resp:
            write_err += 1
        else:
            write_lat.append(dt)
        time.sleep(SLEEP_SEC)

    # Reads
    for _ in range(N_READS):
        dt, resp = call("SELECT COUNT(*) FROM sbtest.writes;", mode)
        if "error_status" in resp:
            read_err += 1
        else:
            read_lat.append(dt)
        time.sleep(SLEEP_SEC)

    return {
        "mode": mode,
        "writes": stats(write_lat),
        "writes_errors": write_err,
        "reads": stats(read_lat),
        "reads_errors": read_err,
    }

def main():
    for mode in ["direct", "random", "ping"]:
        print("=" * 60)
        print(run(mode))

if __name__ == "__main__":
    main()
