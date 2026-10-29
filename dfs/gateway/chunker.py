import hashlib
from fastcdc import fastcdc

FIXED_SIZE = 4 * 1024 * 1024


def fixed_chunks(path, size=FIXED_SIZE):
    with open(path, "rb") as f:
        while True:
            b = f.read(size)
            if not b:
                break
            yield b


def cdc_chunks(path):
    # FastCDC (rolling Gear hash), avg 1MB chunks
    for c in fastcdc(path, min_size=256 * 1024, avg_size=1024 * 1024, max_size=4 * 1024 * 1024, fat=True):
        yield c.data


def chunks(path, mode):
    for b in (cdc_chunks(path) if mode == "cdc" else fixed_chunks(path)):
        yield hashlib.sha256(b).hexdigest(), b
