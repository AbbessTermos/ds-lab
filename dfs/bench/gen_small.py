"""Workload (a): 5000 small files (1-50KB) of JSON/text, ~10% exact duplicates."""
import json, os, random, sys
out = sys.argv[1] if len(sys.argv) > 1 else "data/small"
n = int(sys.argv[2]) if len(sys.argv) > 2 else 5000
random.seed(1)
os.makedirs(out, exist_ok=True)
words = "alpha beta gamma delta storage node chunk replica hash ring cluster data file object".split()
prev = []
for i in range(n):
    if prev and random.random() < 0.1:
        body = random.choice(prev)
    else:
        size = random.randint(1024, 50 * 1024)
        if i % 2:
            body = json.dumps({"id": i, "items": [random.choice(words) for _ in range(size // 8)]})[:size].encode()
        else:
            body = " ".join(random.choice(words) for _ in range(size // 5)).encode()[:size]
        prev.append(body)
    ext = "json" if i % 2 else "txt"
    open(os.path.join(out, f"file_{i:05d}.{ext}"), "wb").write(body)
print("generated", n, "files in", out)
