import time
import boto3
import argparse
import os
import uuid

try:
    import requests
except Exception:
    requests = None


REGION = "ca-central-1"
STACK  = "log8415e-demo3"

ADMIN_CIDR = "142.116.246.18/32"
KEY_NAME   = "log8415e-key"
API_KEY    = "MY_SECRET_KEY_123"

REPO_URL = "https://github.com/LounesSA/log8415e-final-assignment.git"

ec2 = boto3.client("ec2", region_name=REGION)


def ubuntu_ami():
    imgs = ec2.describe_images(
        Owners=["099720109477"],
        Filters=[
            {"Name": "name", "Values": ["ubuntu/images/hvm-ssd/ubuntu-jammy-22.04-amd64-server-*"]},
            {"Name": "state", "Values": ["available"]},
        ],
    )["Images"]
    imgs.sort(key=lambda x: x["CreationDate"], reverse=True)
    return imgs[0]["ImageId"]


def default_vpc_subnet():
    vpc = ec2.describe_vpcs(Filters=[{"Name": "isDefault", "Values": ["true"]}])["Vpcs"][0]["VpcId"]
    subnet = ec2.describe_subnets(Filters=[{"Name": "vpc-id", "Values": [vpc]}])["Subnets"][0]["SubnetId"]
    return vpc, subnet


def ensure_sg(vpc_id, name, desc):
    existing = ec2.describe_security_groups(
        Filters=[{"Name": "group-name", "Values": [name]}, {"Name": "vpc-id", "Values": [vpc_id]}]
    )["SecurityGroups"]
    if existing:
        return existing[0]["GroupId"]
    sg = ec2.create_security_group(GroupName=name, Description=desc, VpcId=vpc_id)["GroupId"]
    ec2.create_tags(Resources=[sg], Tags=[{"Key": "Name", "Value": name}, {"Key": "Stack", "Value": STACK}])
    return sg


def add_ingress(sg_id, perms):
    try:
        ec2.authorize_security_group_ingress(GroupId=sg_id, IpPermissions=perms)
    except Exception as e:
        if "InvalidPermission.Duplicate" in str(e):
            return
        raise


def create_sgs(vpc_id):
    sg_gk = ensure_sg(vpc_id, f"{STACK}-sg-gatekeeper", "Gatekeeper SG")
    sg_px = ensure_sg(vpc_id, f"{STACK}-sg-proxy", "Proxy SG")
    sg_db = ensure_sg(vpc_id, f"{STACK}-sg-db", "DB SG")

    # Gatekeeper: 80 Internet, 22 admin
    add_ingress(sg_gk, [
        {"IpProtocol": "tcp", "FromPort": 80, "ToPort": 80,
         "IpRanges": [{"CidrIp": "0.0.0.0/0"}]},
        {"IpProtocol": "tcp", "FromPort": 22, "ToPort": 22,
         "IpRanges": [{"CidrIp": ADMIN_CIDR}]},
    ])

    # Proxy: 5000 from Gatekeeper SG only, 22 admin
    add_ingress(sg_px, [
        {"IpProtocol": "tcp", "FromPort": 5000, "ToPort": 5000,
         "UserIdGroupPairs": [{"GroupId": sg_gk}]},
        {"IpProtocol": "tcp", "FromPort": 22, "ToPort": 22,
         "IpRanges": [{"CidrIp": ADMIN_CIDR}]},
    ])

    # DB: 3306 from Proxy SG + self, 22 admin
    add_ingress(sg_db, [
        {"IpProtocol": "tcp", "FromPort": 3306, "ToPort": 3306,
         "UserIdGroupPairs": [{"GroupId": sg_px}]},
        {"IpProtocol": "tcp", "FromPort": 3306, "ToPort": 3306,
         "UserIdGroupPairs": [{"GroupId": sg_db}]},
        {"IpProtocol": "tcp", "FromPort": 22, "ToPort": 22,
         "IpRanges": [{"CidrIp": ADMIN_CIDR}]},
    ])

    return sg_gk, sg_px, sg_db


def run_instance(name, ami, itype, subnet, sg_id, user_data):
    r = ec2.run_instances(
        ImageId=ami,
        InstanceType=itype,
        KeyName=KEY_NAME,
        MinCount=1, MaxCount=1,
        NetworkInterfaces=[{
            "DeviceIndex": 0,
            "SubnetId": subnet,
            "Groups": [sg_id],
            "AssociatePublicIpAddress": True
        }],
        TagSpecifications=[{
            "ResourceType": "instance",
            "Tags": [{"Key": "Name", "Value": name}, {"Key": "Stack", "Value": STACK}]
        }],
        UserData=user_data
    )
    return r["Instances"][0]["InstanceId"]


def wait_running(ids):
    ec2.get_waiter("instance_running").wait(InstanceIds=ids)


def get_ips(iid):
    inst = ec2.describe_instances(InstanceIds=[iid])["Reservations"][0]["Instances"][0]
    return inst["PrivateIpAddress"], inst.get("PublicIpAddress", "")


def ud_db_manager():
    return r"""#!/usr/bin/env bash
set -euo pipefail
export DEBIAN_FRONTEND=noninteractive
apt-get update -y
apt-get install -y mysql-server wget unzip

CNF="/etc/mysql/mysql.conf.d/mysqld.cnf"
sed -i 's/^bind-address.*/bind-address = 0.0.0.0/' "$CNF" || true

grep -q '^server-id' "$CNF" || echo 'server-id = 1' >> "$CNF"
grep -q '^log_bin' "$CNF" || echo 'log_bin = mysql-bin' >> "$CNF"
grep -q '^binlog_format' "$CNF" || echo 'binlog_format = ROW' >> "$CNF"
grep -q '^gtid_mode' "$CNF" || echo 'gtid_mode = ON' >> "$CNF"
grep -q '^enforce_gtid_consistency' "$CNF" || echo 'enforce_gtid_consistency = ON' >> "$CNF"
grep -q '^log_replica_updates' "$CNF" || echo 'log_replica_updates = ON' >> "$CNF"

systemctl restart mysql

# Users (mysql_native_password avoids "secure connection required" issues)
mysql -e "CREATE USER IF NOT EXISTS 'repl'@'%' IDENTIFIED WITH mysql_native_password BY 'ReplPass123!';"
mysql -e "GRANT REPLICATION SLAVE ON *.* TO 'repl'@'%';"

mysql -e "CREATE USER IF NOT EXISTS 'app'@'%' IDENTIFIED WITH mysql_native_password BY 'AppPass123!';"
mysql -e "CREATE USER IF NOT EXISTS 'sb'@'localhost' IDENTIFIED WITH mysql_native_password BY 'SbPass123!';"
mysql -e "GRANT ALL PRIVILEGES ON *.* TO 'sb'@'localhost';"
mysql -e "FLUSH PRIVILEGES;"

# Sakila
if ! mysql -e "USE sakila;" >/dev/null 2>&1; then
  cd /tmp
  wget -q https://downloads.mysql.com/docs/sakila-db.zip -O sakila-db.zip
  unzip -o sakila-db.zip
  mysql < sakila-db/sakila-schema.sql
  mysql < sakila-db/sakila-data.sql
fi

# sbtest DB used by your bench (writes)
mysql -e "CREATE DATABASE IF NOT EXISTS sbtest;"
mysql -e "USE sbtest; CREATE TABLE IF NOT EXISTS writes (id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY, name VARCHAR(64) NOT NULL, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP) ENGINE=InnoDB;"

mysql -e "GRANT SELECT,INSERT,UPDATE,DELETE ON sakila.* TO 'app'@'%';"
mysql -e "GRANT SELECT,INSERT,UPDATE,DELETE ON sbtest.* TO 'app'@'%';"
mysql -e "FLUSH PRIVILEGES;"
"""


def ud_db_worker(server_id, manager_ip):
    return f"""#!/usr/bin/env bash
set -euo pipefail
export DEBIAN_FRONTEND=noninteractive
apt-get update -y
apt-get install -y mysql-server

CNF="/etc/mysql/mysql.conf.d/mysqld.cnf"
sed -i 's/^bind-address.*/bind-address = 0.0.0.0/' "$CNF" || true
sed -i 's/^server-id.*/server-id = {server_id}/' "$CNF" || true
grep -q '^server-id' "$CNF" || echo 'server-id = {server_id}' >> "$CNF"

grep -q '^log_bin' "$CNF" || echo 'log_bin = mysql-bin' >> "$CNF"
grep -q '^relay_log' "$CNF" || echo 'relay_log = mysql-relay-bin' >> "$CNF"
grep -q '^binlog_format' "$CNF" || echo 'binlog_format = ROW' >> "$CNF"
grep -q '^gtid_mode' "$CNF" || echo 'gtid_mode = ON' >> "$CNF"
grep -q '^enforce_gtid_consistency' "$CNF" || echo 'enforce_gtid_consistency = ON' >> "$CNF"
grep -q '^log_replica_updates' "$CNF" || echo 'log_replica_updates = ON' >> "$CNF"
grep -q '^read_only' "$CNF" || echo 'read_only = ON' >> "$CNF"

systemctl restart mysql

mysql -e "STOP REPLICA;" >/dev/null 2>&1 || true
mysql -e "RESET REPLICA ALL;" >/dev/null 2>&1 || true

mysql -e "CHANGE REPLICATION SOURCE TO SOURCE_HOST='{manager_ip}', SOURCE_USER='repl', SOURCE_PASSWORD='ReplPass123!', SOURCE_AUTO_POSITION=1;"
mysql -e "START REPLICA;" || mysql -e "START SLAVE;"
"""


def ud_proxy(manager_ip, w1, w2):
    return f"""#!/usr/bin/env bash
set -euo pipefail
export DEBIAN_FRONTEND=noninteractive
apt-get update -y
apt-get install -y git python3-venv python3-pip

APP=/home/ubuntu/app
if [ ! -d "$APP" ]; then
  sudo -u ubuntu git clone {REPO_URL} "$APP"
else
  sudo -u ubuntu bash -lc "cd $APP && git pull"
fi

sudo -u ubuntu bash -lc "cd $APP && python3 -m venv .venv && . .venv/bin/activate && pip install -U pip"
sudo -u ubuntu bash -lc "cd $APP && . .venv/bin/activate && pip install -r requirements.txt mysql-connector-python"

cat >/etc/log8415e_proxy.env <<ENV
MANAGER_HOST={manager_ip}
WORKER1_HOST={w1}
WORKER2_HOST={w2}
MYSQL_USER=app
MYSQL_PASS=AppPass123!
ENV

cat >/etc/systemd/system/log8415e-proxy.service <<'SVC'
[Unit]
Description=LOG8415E Proxy
After=network-online.target
Wants=network-online.target
[Service]
WorkingDirectory=/home/ubuntu/app
EnvironmentFile=/etc/log8415e_proxy.env
ExecStart=/home/ubuntu/app/.venv/bin/python3 /home/ubuntu/app/proxy/proxy.py
Restart=always
RestartSec=2
[Install]
WantedBy=multi-user.target
SVC

systemctl daemon-reload
systemctl enable log8415e-proxy
systemctl restart log8415e-proxy
"""


def ud_gatekeeper(proxy_ip):
    return f"""#!/usr/bin/env bash
set -euo pipefail
export DEBIAN_FRONTEND=noninteractive
apt-get update -y
apt-get install -y git python3-venv python3-pip

APP=/home/ubuntu/app
if [ ! -d "$APP" ]; then
  sudo -u ubuntu git clone {REPO_URL} "$APP"
else
  sudo -u ubuntu bash -lc "cd $APP && git pull"
fi

sudo -u ubuntu bash -lc "cd $APP && python3 -m venv .venv && . .venv/bin/activate && pip install -U pip"
sudo -u ubuntu bash -lc "cd $APP && . .venv/bin/activate && pip install -r requirements.txt requests"

cat >/etc/log8415e_gatekeeper.env <<ENV
PROXY_URL=http://{proxy_ip}:5000/query
API_KEY={API_KEY}
ENV

cat >/etc/systemd/system/log8415e-gatekeeper.service <<'SVC'
[Unit]
Description=LOG8415E Gatekeeper
After=network-online.target
Wants=network-online.target
[Service]
WorkingDirectory=/home/ubuntu/app
EnvironmentFile=/etc/log8415e_gatekeeper.env
ExecStart=/home/ubuntu/app/.venv/bin/python3 /home/ubuntu/app/gatekeeper/gatekeeper.py
Restart=always
RestartSec=2
[Install]
WantedBy=multi-user.target
SVC

systemctl daemon-reload
systemctl enable log8415e-gatekeeper
systemctl restart log8415e-gatekeeper
"""


def percentile(sorted_vals, p):
    if not sorted_vals:
        return None
    idx = int(p * (len(sorted_vals) - 1))
    return sorted_vals[idx]


def run_bench(gk_pub_ip, n):
    if requests is None:
        raise RuntimeError("Missing 'requests'. Install: python3 -m pip install --user requests")

    url = f"http://{gk_pub_ip}/query"
    headers = {"Content-Type": "application/json", "X-API-Key": API_KEY}

    def one_mode(mode, sql, count):
        times = []
        errors = 0
        for _ in range(count):
            req_id = str(uuid.uuid4())
            t0 = time.time()
            try:
                r = requests.post(url, headers=headers, json={"sql": sql, "mode": mode, "request_id": req_id}, timeout=15)
                dt = (time.time() - t0) * 1000.0
                if r.status_code >= 400:
                    errors += 1
                else:
                    times.append(dt)
            except Exception:
                errors += 1
        times.sort()
        return {
            "count": len(times),
            "errors": errors,
            "avg_ms": (sum(times) / len(times)) if times else None,
            "p50_ms": percentile(times, 0.50),
            "p95_ms": percentile(times, 0.95),
            "max_ms": max(times) if times else None,
        }

    read_sql  = "SELECT COUNT(*) FROM sbtest.writes;"
    write_sql = "INSERT INTO sbtest.writes(name) VALUES(CONCAT('Bench_', UUID()));"

    results = {}
    for mode in ["direct", "random", "ping"]:
        results[mode] = {
            "reads": one_mode(mode, read_sql, n),
            "writes": one_mode(mode, write_sql, n),
        }

    os.makedirs("docs", exist_ok=True)
    out = f"docs/benchmark_{n}.txt"
    with open(out, "w") as f:
        f.write(str(results) + "\n")

    print("\nBENCHMARK ✅")
    print("Saved to:", out)
    print(results)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bench", action="store_true", help="Run benchmark against Gatekeeper after provisioning")
    parser.add_argument("--n", type=int, default=1000, help="Number of reads and writes per mode")
    args = parser.parse_args()

    ami = ubuntu_ami()
    vpc, subnet = default_vpc_subnet()
    sg_gk, sg_px, sg_db = create_sgs(vpc)

    print("[1] Launch DB manager...")
    dbm = run_instance(f"{STACK}-db-manager", ami, "t2.micro", subnet, sg_db, ud_db_manager())
    wait_running([dbm])
    dbm_priv, dbm_pub = get_ips(dbm)
    print("    db-manager", dbm_priv, dbm_pub)

    print("[2] Launch DB workers...")
    w1 = run_instance(f"{STACK}-db-worker1", ami, "t2.micro", subnet, sg_db, ud_db_worker(2, dbm_priv))
    w2 = run_instance(f"{STACK}-db-worker2", ami, "t2.micro", subnet, sg_db, ud_db_worker(3, dbm_priv))
    wait_running([w1, w2])
    w1_priv, w1_pub = get_ips(w1)
    w2_priv, w2_pub = get_ips(w2)
    print("    worker1", w1_priv, w1_pub)
    print("    worker2", w2_priv, w2_pub)

    print("[3] Launch Proxy (t2.large)...")
    px = run_instance(f"{STACK}-proxy", ami, "t2.large", subnet, sg_px, ud_proxy(dbm_priv, w1_priv, w2_priv))
    wait_running([px])
    px_priv, px_pub = get_ips(px)
    print("    proxy", px_priv, px_pub)

    print("[4] Launch Gatekeeper (t2.large)...")
    gk = run_instance(f"{STACK}-gatekeeper", ami, "t2.large", subnet, sg_gk, ud_gatekeeper(px_priv))
    wait_running([gk])
    gk_priv, gk_pub = get_ips(gk)
    print("    gatekeeper", gk_priv, gk_pub)

    print("\nDONE")
    print("Gatekeeper health:", f"http://{gk_pub}/health")
    print("Gatekeeper query :", f"http://{gk_pub}/query")
    print("Test:")
    print(f'curl -X POST http://{gk_pub}/query -H "Content-Type: application/json" -H "X-API-Key: {API_KEY}" '
          f'-d \'{{"sql":"SELECT COUNT(*) FROM sakila.actor;","mode":"random"}}\'')

    if args.bench:
        print("\nRunning benchmark... (this can take a while)")
        run_bench(gk_pub, args.n)


if __name__ == "__main__":
    main()