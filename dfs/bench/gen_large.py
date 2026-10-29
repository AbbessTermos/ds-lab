"""Workload (b): 10 large files (100MB..MAX_MB), like disk-image versions:
each file = a shared random base with a few random bytes inserted at a random place, plus a unique tail."""
import os, random, sys
out = sys.argv[1] if len(sys.argv) > 1 else "data/large"
max_mb = int(sys.argv[2]) if len(sys.argv) > 2 else 300
MB = 1024 * 1024
random.seed(2)
os.makedirs(out, exist_ok=True)
base = os.urandom(100 * MB)
for i in range(10):
    size = (100 + (max_mb - 100) * i // 9) * MB
    pos = random.randint(0, len(base))
    with open(os.path.join(out, f"image_{i:02d}.img"), "wb") as f:
        f.write(base[:pos]); f.write(os.urandom(random.randint(1, 5000))); f.write(base[pos:])
        rest = size - f.tell()
        while rest > 0:
            f.write(os.urandom(min(rest, 16 * MB))); rest -= 16 * MB
    print("wrote", f"image_{i:02d}.img", size // MB, "MB")
