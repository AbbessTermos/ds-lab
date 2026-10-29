"""Placement test: % of chunks that move when a node is added / removed (consistent hashing, 100 vnodes)."""
import hashlib, json, os, sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "gateway"))
from ring import Ring

N, CHUNKS = 3, 100000
hashes = [hashlib.sha256(str(i).encode()).hexdigest() for i in range(CHUNKS)]
nodes = [f"node{i}" for i in range(1, 7)]


def moved(a, b):
    prim = sum(a.lookup(h, 1) != b.lookup(h, 1) for h in hashes) / CHUNKS * 100
    reps = sum(len(set(a.lookup(h, N)) - set(b.lookup(h, N))) for h in hashes) / (CHUNKS * N) * 100
    return round(prim, 2), round(reps, 2)


base = Ring(nodes)
res = {"add node7": moved(base, Ring(nodes + ["node7"])), "remove node3": moved(base, Ring([n for n in nodes if n != "node3"]))}
res["ideal add (1/7)"] = round(100 / 7, 2)
res["ideal remove (1/6)"] = round(100 / 6, 2)
load = {n: 0 for n in nodes}
for h in hashes:
    for n in base.lookup(h, N):
        load[n] += 1
print("case: (% primary moved, % replicas moved)")
for k, v in res.items():
    print(f"  {k}: {v}")
print("replicas per node:", load)
os.makedirs(os.path.join(os.path.dirname(__file__), "results"), exist_ok=True)
json.dump({"moves": res, "load": load}, open(os.path.join(os.path.dirname(__file__), "results", "ring_test.json"), "w"), indent=2)
