import hashlib, os, shutil, threading, time
import requests
from fastapi import FastAPI, Request, Response, HTTPException

NODE_ID = os.environ.get("NODE_ID", "node1")
NODE_URL = os.environ.get("NODE_URL", "http://node1:8000")
META_URL = os.environ.get("META_URL", "http://gateway:8000")
DATA = os.environ.get("DATA_DIR", "/data")
HEARTBEAT_SEC = float(os.environ.get("HEARTBEAT_SEC", "3"))
os.makedirs(DATA, exist_ok=True)
app = FastAPI(title=f"storage node {NODE_ID}")


def path(h):
    if len(h) != 64 or any(c not in "0123456789abcdef" for c in h):
        raise HTTPException(400, "bad hash")
    return os.path.join(DATA, h[:2], h)


@app.put("/chunks/{h}")
async def put_chunk(h: str, request: Request):
    data = await request.body()
    if hashlib.sha256(data).hexdigest() != h:
        raise HTTPException(400, "hash mismatch")
    p = path(h)
    os.makedirs(os.path.dirname(p), exist_ok=True)
    if not os.path.exists(p):
        with open(p + ".tmp", "wb") as f:
            f.write(data)
        os.replace(p + ".tmp", p)
    return {"stored": h}


@app.get("/chunks/{h}")
def get_chunk(h: str):
    p = path(h)
    if not os.path.exists(p):
        raise HTTPException(404, "not found")
    data = open(p, "rb").read()
    if hashlib.sha256(data).hexdigest() != h:  # verify on every read
        raise HTTPException(409, "corrupted chunk")
    return Response(data, media_type="application/octet-stream")


@app.head("/chunks/{h}")
def head_chunk(h: str):
    p = path(h)
    if not os.path.exists(p):
        raise HTTPException(404)
    return Response(headers={"Content-Length": str(os.path.getsize(p))})


@app.delete("/chunks/{h}")
def delete_chunk(h: str):
    p = path(h)
    if os.path.exists(p):
        os.remove(p)
    return {"deleted": h}


@app.get("/health")
def health():
    """Used by Kubernetes liveness / readiness probes."""
    open(os.path.join(DATA, ".probe"), "w").write("ok")
    return {"node": NODE_ID, "status": "ok"}


def count_chunks():
    return sum(len(fs) for d, _, fs in os.walk(DATA) if d != DATA)


def heartbeat():
    while True:
        try:
            requests.post(f"{META_URL}/internal/heartbeat", timeout=2, json={
                "id": NODE_ID, "url": NODE_URL,
                "free": shutil.disk_usage(DATA).free, "chunks": count_chunks()})
        except Exception:
            pass
        time.sleep(HEARTBEAT_SEC)


threading.Thread(target=heartbeat, daemon=True).start()
