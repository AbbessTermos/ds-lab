"""Benchmark: upload + download a workload with replication and erasure coding (fixed chunks),
then FastCDC; measure throughput, dedup, storage overhead and repair traffic after stopping a node.
usage: python bench/run_bench.py small data/small     |   python bench/run_bench.py large data/large"""
import json, os, subprocess, sys, time
from concurrent.futures import ThreadPoolExecutor
import requests

URL = os.environ.get("GATEWAY_URL", "http://localhost:8080")
H = {"X-API-Key": "admin-key"}
name, folder = sys.argv[1], sys.argv[2]
files = sorted(os.listdir(folder))
threads = 16 if name == "small" else 2
S = requests.Session()
S.mount("http://", requests.adapters.HTTPAdapter(pool_maxsize=32))


def status():
    return S.get(f"{URL}/status", headers=H).json()


def upload(bucket, f, chunking):
    with open(os.path.join(folder, f), "rb") as fh:
        r = S.put(f"{URL}/buckets/{bucket}/objects/{f}", params={"chunking": chunking, "tags": f"workload={name}"}, headers=H, data=fh)
    r.raise_for_status()


def download(bucket, f):
    return len(S.get(f"{URL}/buckets/{bucket}/objects/{f}", headers=H).content)


def repair_test(victim="node6"):
    while status()["degraded_chunks"]:
        time.sleep(2)
    before = status()["repair"]
    subprocess.run(["docker", "stop", victim], check=True, capture_output=True)
    t = time.time()
    time.sleep(15)
    while True:
        s = status()
        if s["degraded_chunks"] == 0 or time.time() - t > 1800:
            break
        time.sleep(3)
    took = time.time() - t
    after = s["repair"]
    subprocess.run(["docker", "start", victim], check=True, capture_output=True)
    while not any(n["id"] == victim and n["status"] == "up" for n in status()["nodes"]):
        time.sleep(2)
    d = lambda k: after.get(k, 0) - before.get(k, 0)
    return {"repair_time_s": round(took, 1), "repaired_chunks": d("repaired_chunks"),
            "repair_read_MB": round(d("repair_bytes_read") / 1e6, 2), "repair_written_MB": round(d("repair_bytes_written") / 1e6, 2)}


results = []
for mode, chunking, do_repair in [("rep", "fixed", True), ("ec", "fixed", True), ("rep", "cdc", False)]:
    bucket = f"{name}-{mode}-{chunking}-{int(time.time())}"
    S.post(f"{URL}/buckets/{bucket}", params={"mode": mode}, headers=H)
    t = time.time()
    with ThreadPoolExecutor(threads) as ex:
        list(ex.map(lambda f: upload(bucket, f, chunking), files))
    up = time.time() - t
    t = time.time()
    with ThreadPoolExecutor(threads) as ex:
        total = sum(ex.map(lambda f: download(bucket, f), files))
    down = time.time() - t
    st = S.get(f"{URL}/buckets/{bucket}/stats", headers=H).json()
    r = {"workload": name, "mode": mode, "chunking": chunking, "files": len(files),
         "logical_MB": round(st["logical_bytes"] / 1e6, 1), "unique_chunks": st["unique_chunks"],
         "dedup_ratio": st["dedup_ratio"], "physical_MB": round(st["physical_bytes"] / 1e6, 1),
         "storage_overhead": st["storage_overhead"],
         "upload_s": round(up, 1), "upload_MBps": round(total / 1e6 / up, 1), "upload_files_per_s": round(len(files) / up, 1),
         "download_s": round(down, 1), "download_MBps": round(total / 1e6 / down, 1)}
    if do_repair:
        r.update(repair_test())
    print(json.dumps(r))
    results.append(r)

os.makedirs("bench/results", exist_ok=True)
json.dump(results, open(f"bench/results/{name}.json", "w"), indent=2)
cols = list(results[0].keys())
with open(f"bench/results/{name}.csv", "w") as f:
    f.write(",".join(cols) + "\n")
    for r in results:
        f.write(",".join(str(r.get(c, "")) for c in cols) + "\n")
print("saved bench/results/" + name + ".json/.csv")
