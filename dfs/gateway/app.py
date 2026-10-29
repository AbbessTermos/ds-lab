import hashlib, json, os, re, socket, tempfile, threading, time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from datetime import datetime, timezone
from urllib.parse import quote

import psycopg2, psycopg2.pool, requests
from fastapi import Depends, FastAPI, Header, HTTPException, Request
from fastapi.responses import RedirectResponse, Response, StreamingResponse
from fastapi.staticfiles import StaticFiles
from starlette.concurrency import run_in_threadpool
from zfec import easyfec
from prometheus_client import Counter, Gauge, Histogram, generate_latest, CONTENT_TYPE_LATEST

import mq

from chunker import chunks
from ring import Ring

N = int(os.environ.get("REPLICATION_N", "3"))
W = int(os.environ.get("WRITE_QUORUM", "2"))
R = int(os.environ.get("READ_QUORUM", "2"))
K = int(os.environ.get("EC_K", "4"))
M = int(os.environ.get("EC_M", "2"))
FAIL_AFTER = float(os.environ.get("FAIL_AFTER_SEC", "10"))
REPAIR_EVERY = float(os.environ.get("REPAIR_EVERY_SEC", "5"))
SCRUB_EVERY = float(os.environ.get("SCRUB_EVERY_SEC", "300"))
ES_URL = os.environ.get("ES_URL", "")
# "key:limit_per_minute,key2:limit"
RUN_BACKGROUND = os.environ.get("RUN_BACKGROUND", "1") == "1"  # only one gateway runs repair/scrub loops
API_KEYS = dict((k.split(":")[0], int(k.split(":")[1])) for k in os.environ.get("API_KEYS", "admin-key:100000").split(","))
assert W + R > N, "quorum requires W+R>N"

app = FastAPI(title="DFS gateway")
app.mount("/ui", StaticFiles(directory=os.path.join(os.path.dirname(__file__), "static"), html=True), name="ui")
http = requests.Session()
http.mount("http://", requests.adapters.HTTPAdapter(pool_maxsize=50))
pool = ThreadPoolExecutor(16)
_pool = None


# ---------------- database ----------------
def _live_conn():
    """Get a pooled connection that still works (dead ones are dropped, e.g. after a database failover)."""
    for _ in range(40):
        conn = _pool.getconn()
        try:
            if not conn.closed:
                with conn.cursor() as c:
                    c.execute("SELECT 1")
                conn.rollback()
                return conn
        except psycopg2.Error:
            pass
        _pool.putconn(conn, close=True)
    raise HTTPException(503, "metadata database unavailable")


@contextmanager
def db():
    conn = _live_conn()
    try:
        with conn, conn.cursor() as cur:
            yield cur
    except (psycopg2.OperationalError, psycopg2.InterfaceError):
        _pool.putconn(conn, close=True)  # e.g. database failover: drop the dead connection
        conn = None
        raise
    finally:
        if conn is not None:
            _pool.putconn(conn)


def bump(name, n=1):
    """Shared counters (repair / scrub / jobs) kept in PostgreSQL so all gateways and workers see them."""
    if n:
        with db() as c:
            c.execute("INSERT INTO counters VALUES(%s,%s) ON CONFLICT(name) DO UPDATE SET value=counters.value+EXCLUDED.value", (name, n))


def counters():
    with db() as c:
        c.execute("SELECT name, value FROM counters")
        return dict(c.fetchall())


def init_db():
    global _pool
    for _ in range(30):
        try:
            _pool = psycopg2.pool.ThreadedConnectionPool(1, 30, os.environ.get("DATABASE_URL", "postgresql://dfs:dfs@postgres/dfs"))
            break
        except psycopg2.OperationalError:
            time.sleep(2)
    with db() as c:
        c.execute("SELECT pg_advisory_xact_lock(42)")  # several gateways/workers start at the same time
        c.execute("""
        CREATE TABLE IF NOT EXISTS buckets(name text PRIMARY KEY, mode text NOT NULL, created timestamptz DEFAULT now());
        CREATE TABLE IF NOT EXISTS objects(bucket text, key text, version int, size bigint, chunks jsonb,
            created timestamptz DEFAULT now(), tags jsonb DEFAULT '{}', PRIMARY KEY(bucket, key));
        CREATE TABLE IF NOT EXISTS chunks(hash text, kind text, size bigint, refcount int, loc jsonb,
            PRIMARY KEY(hash, kind));
        CREATE TABLE IF NOT EXISTS nodes(id text PRIMARY KEY, url text, free bigint, chunks int, last_seen double precision);
        CREATE TABLE IF NOT EXISTS counters(name text PRIMARY KEY, value bigint);
        CREATE TABLE IF NOT EXISTS ratelimit(key text, minute bigint, n int, PRIMARY KEY(key, minute));
        """)


# ---------------- auth + rate limit ----------------
def auth(x_api_key: str = Header(None)):
    if x_api_key not in API_KEYS:
        raise HTTPException(401, "invalid API key")
    # fixed 1-minute window, stored in PostgreSQL so the limit is shared by all gateway instances
    with db() as c:
        c.execute("""INSERT INTO ratelimit VALUES(%s,%s,1) ON CONFLICT(key, minute)
                     DO UPDATE SET n=ratelimit.n+1 RETURNING n""", (x_api_key, int(time.time() // 60)))
        n = c.fetchone()[0]
    if n > API_KEYS[x_api_key]:
        raise HTTPException(429, "rate limit exceeded")


# ---------------- nodes ----------------
def all_nodes():
    with db() as c:
        c.execute("SELECT id, url, free, chunks, last_seen FROM nodes ORDER BY id")
        return c.fetchall()


def alive():
    now = time.time()
    return {n[0]: n[1] for n in all_nodes() if now - n[4] < FAIL_AFTER}


@app.post("/internal/heartbeat")
def heartbeat(hb: dict):
    with db() as c:
        c.execute("""INSERT INTO nodes VALUES(%s,%s,%s,%s,%s) ON CONFLICT(id) DO UPDATE
                     SET url=EXCLUDED.url, free=EXCLUDED.free, chunks=EXCLUDED.chunks, last_seen=EXCLUDED.last_seen
                     RETURNING (xmax=0)""",
                  (hb["id"], hb["url"], hb["free"], hb["chunks"], time.time()))
        new_node = c.fetchone()[0]
    if new_node and mq.URL:
        threading.Thread(target=enqueue_rebalance, daemon=True).start()  # new node joined -> move chunks onto it
    return {"ok": True}


def node_put(url, h, data):
    try:
        return http.put(f"{url}/chunks/{h}", data=data, timeout=30).ok
    except requests.RequestException:
        return False


def node_get(url, h):
    try:
        r = http.get(f"{url}/chunks/{h}", timeout=30)
        return r.content if r.ok else None
    except requests.RequestException:
        return None


def node_head(url, h):
    try:
        return http.head(f"{url}/chunks/{h}", timeout=5).ok
    except requests.RequestException:
        return False


def node_delete(url, h):
    try:
        http.delete(f"{url}/chunks/{h}", timeout=5)
    except requests.RequestException:
        pass


# ---------------- chunk storage: replication / erasure coding ----------------
def store_chunk(h, data, kind, nodes):
    ring = Ring(list(nodes))
    if kind == "rep":
        targets = ring.lookup(h, N)
        ok = list(pool.map(lambda n: node_put(nodes[n], h, data), targets))
        loc = [n for n, s in zip(targets, ok) if s]
        if len(loc) < W:
            raise HTTPException(503, f"write quorum not reached ({len(loc)}/{W})")
        return loc
    shares = easyfec.Encoder(K, K + M).encode(data)
    padlen = len(shares[0]) * K - len(data)
    targets = ring.lookup(h, K + M)
    targets = [targets[i % len(targets)] for i in range(K + M)]
    entries = [{"i": i, "h": hashlib.sha256(s).hexdigest(), "node": targets[i]} for i, s in enumerate(shares)]
    ok = list(pool.map(lambda e: node_put(nodes[e["node"]], e["h"], shares[e["i"]]), entries))
    entries = [e for e, s in zip(entries, ok) if s]
    if len(entries) < K + 1:
        raise HTTPException(503, "not enough nodes for erasure coding")
    return {"padlen": padlen, "shares": entries}


def read_chunk(h, kind, loc, nodes):
    if kind == "rep":
        data, ok = None, 0
        for n in [n for n in loc if n in nodes]:
            if data is None:
                data = node_get(nodes[n], h)
                ok += data is not None
            else:
                ok += node_head(nodes[n], h)
            if ok >= R:
                return data
        if data is not None and ok >= min(R, len(loc)):
            return data
        raise HTTPException(503, f"read quorum not reached for chunk {h[:12]}")
    blocks, nums = [], []
    for e in loc["shares"]:
        if e["node"] in nodes and len(blocks) < K:
            b = node_get(nodes[e["node"]], e["h"])
            if b is not None:
                blocks.append(b); nums.append(e["i"])
    if len(blocks) < K:
        raise HTTPException(503, f"cannot decode chunk {h[:12]}")
    return easyfec.Decoder(K, K + M).decode(blocks, nums, loc["padlen"])


def add_chunk(h, data, kind, nodes):
    """Dedup: store the chunk only if it is new, otherwise just increase refcount."""
    with db() as c:
        c.execute("""INSERT INTO chunks VALUES(%s,%s,%s,1,'null') ON CONFLICT(hash,kind)
                     DO UPDATE SET refcount=chunks.refcount+1 RETURNING (xmax=0)""", (h, kind, len(data)))
        new = c.fetchone()[0]
    if new:
        try:
            loc = store_chunk(h, data, kind, nodes)
        except Exception:
            with db() as c:
                c.execute("DELETE FROM chunks WHERE hash=%s AND kind=%s", (h, kind))
            raise
        with db() as c:
            c.execute("UPDATE chunks SET loc=%s WHERE hash=%s AND kind=%s", (json.dumps(loc), h, kind))
    return new


def release_chunks(hashes, kind):
    """Decrease refcount; delete chunk from nodes when nobody references it."""
    nodes = {n[0]: n[1] for n in all_nodes()}
    for h in hashes:
        with db() as c:
            c.execute("UPDATE chunks SET refcount=refcount-1 WHERE hash=%s AND kind=%s RETURNING refcount, loc", (h, kind))
            row = c.fetchone()
            if row and row[0] <= 0:
                c.execute("DELETE FROM chunks WHERE hash=%s AND kind=%s", (h, kind))
                loc = row[1]
                items = [(n, h) for n in loc] if kind == "rep" else [(e["node"], e["h"]) for e in loc["shares"]]
                if mq.URL:
                    mq.publish([{"type": "gc", "items": items}])  # garbage collection done by workers
                else:
                    for n, ch in items:
                        if n in nodes:
                            node_delete(nodes[n], ch)


# ---------------- elasticsearch ----------------
es_pool = ThreadPoolExecutor(2)


def es_index(doc):
    if ES_URL:
        es_pool.submit(_es_index, doc)  # index in the background so uploads never wait for Elasticsearch


def _es_index(doc):
    if ES_URL:
        try:
            http.put(f"{ES_URL}/objects/_doc/{quote(doc['bucket'] + '/' + doc['key'], safe='')}", json=doc, timeout=5)
        except requests.RequestException:
            pass


def es_delete(bucket, key):
    if ES_URL:
        try:
            http.delete(f"{ES_URL}/objects/_doc/{quote(bucket + '/' + key, safe='')}", timeout=5)
        except requests.RequestException:
            pass


# ---------------- API ----------------
@app.get("/")
def root():
    return RedirectResponse("/ui/")


@app.post("/buckets/{bucket}", dependencies=[Depends(auth)])
def create_bucket(bucket: str, mode: str = "rep"):
    if mode not in ("rep", "ec"):
        raise HTTPException(400, "mode must be rep or ec")
    with db() as c:
        c.execute("INSERT INTO buckets(name, mode) VALUES(%s,%s) ON CONFLICT DO NOTHING RETURNING name", (bucket, mode))
        if not c.fetchone():
            raise HTTPException(409, "bucket exists")
    return {"bucket": bucket, "mode": mode}


@app.get("/buckets", dependencies=[Depends(auth)])
def list_buckets():
    with db() as c:
        c.execute("SELECT name, mode, created FROM buckets ORDER BY name")
        return [{"name": r[0], "mode": r[1], "created": r[2].isoformat()} for r in c.fetchall()]


def bucket_mode(bucket):
    with db() as c:
        c.execute("SELECT mode FROM buckets WHERE name=%s", (bucket,))
        r = c.fetchone()
    if not r:
        raise HTTPException(404, "no such bucket")
    return r[0]


@app.put("/buckets/{bucket}/objects/{key:path}", dependencies=[Depends(auth)])
async def put_object(bucket: str, key: str, request: Request, chunking: str = "fixed", tags: str = ""):
    """Streaming upload: body is streamed to a temp file, then chunked."""
    kind = bucket_mode(bucket)
    tagd = dict(t.split("=", 1) for t in tags.split(",") if "=" in t)
    with tempfile.NamedTemporaryFile(delete=False) as f:
        async for part in request.stream():
            f.write(part)
        tmp = f.name
    def process():
        nodes = alive()
        clist, size, new = [], 0, 0
        for h, b in chunks(tmp, chunking):
            new += add_chunk(h, b, kind, nodes)
            clist.append([h, len(b)])
            size += len(b)
        return clist, size, new
    try:
        clist, size, new = await run_in_threadpool(process)
    finally:
        os.remove(tmp)
    BYTES_UP.inc(size)
    with db() as c:
        c.execute("SELECT version, chunks FROM objects WHERE bucket=%s AND key=%s", (bucket, key))
        old = c.fetchone()
        version = old[0] + 1 if old else 1
        c.execute("""INSERT INTO objects(bucket,key,version,size,chunks,tags) VALUES(%s,%s,%s,%s,%s,%s)
                     ON CONFLICT(bucket,key) DO UPDATE SET version=EXCLUDED.version, size=EXCLUDED.size,
                     chunks=EXCLUDED.chunks, tags=EXCLUDED.tags, created=now() RETURNING created""",
                  (bucket, key, version, size, json.dumps(clist), json.dumps(tagd)))
        created = c.fetchone()[0]
    if old:
        release_chunks([x[0] for x in old[1]], kind)
    es_index({"bucket": bucket, "key": key, "size": size, "created": created.isoformat(), "tags": tagd})
    return {"bucket": bucket, "key": key, "version": version, "size": size, "chunks": len(clist), "new_chunks": new}


def get_meta(bucket, key):
    with db() as c:
        c.execute("SELECT version,size,chunks,created,tags FROM objects WHERE bucket=%s AND key=%s", (bucket, key))
        r = c.fetchone()
    if not r:
        raise HTTPException(404, "no such object")
    return {"bucket": bucket, "key": key, "version": r[0], "size": r[1], "chunks": r[2],
            "created": r[3].isoformat(), "tags": r[4]}


@app.get("/buckets/{bucket}/objects", dependencies=[Depends(auth)])
def list_objects(bucket: str, prefix: str = "", limit: int = 1000):
    bucket_mode(bucket)
    with db() as c:
        c.execute("SELECT key,size,version,created FROM objects WHERE bucket=%s AND key LIKE %s ORDER BY key LIMIT %s",
                  (bucket, prefix.replace("%", r"\%") + "%", limit))
        return [{"key": r[0], "size": r[1], "version": r[2], "created": r[3].isoformat()} for r in c.fetchall()]


@app.get("/buckets/{bucket}/meta/{key:path}", dependencies=[Depends(auth)])
def object_meta(bucket: str, key: str):
    m = get_meta(bucket, key)
    m["chunk_count"] = len(m.pop("chunks"))
    return m


@app.get("/buckets/{bucket}/objects/{key:path}", dependencies=[Depends(auth)])
def get_object(bucket: str, key: str, range: str = Header(None)):
    kind = bucket_mode(bucket)
    m = get_meta(bucket, key)
    size, start, end, status = m["size"], 0, m["size"] - 1, 200
    if range:
        mt = re.match(r"bytes=(\d*)-(\d*)", range)
        if not mt:
            raise HTTPException(416)
        a, b = mt.groups()
        start, end = (size - int(b), size - 1) if a == "" else (int(a), int(b) if b else size - 1)
        end = min(end, size - 1)
        if start > end or start >= size:
            raise HTTPException(416, "range not satisfiable")
        status = 206
    hashes = [x[0] for x in m["chunks"]]
    with db() as c:
        c.execute("SELECT hash, loc FROM chunks WHERE kind=%s AND hash = ANY(%s)", (kind, hashes))
        locs = dict(c.fetchall())
    nodes = alive()

    def gen():
        off = 0
        for h, sz in m["chunks"]:
            if off + sz > start and off <= end:
                data = read_chunk(h, kind, locs[h], nodes)
                part = data[max(0, start - off): end - off + 1]
                BYTES_DOWN.inc(len(part))
                yield part
            off += sz

    headers = {"Content-Length": str(end - start + 1), "Accept-Ranges": "bytes", "X-Version": str(m["version"])}
    if status == 206:
        headers["Content-Range"] = f"bytes {start}-{end}/{size}"
    return StreamingResponse(gen(), status_code=status, headers=headers, media_type="application/octet-stream")


@app.delete("/buckets/{bucket}/objects/{key:path}", dependencies=[Depends(auth)])
def delete_object(bucket: str, key: str):
    kind = bucket_mode(bucket)
    with db() as c:
        c.execute("DELETE FROM objects WHERE bucket=%s AND key=%s RETURNING chunks", (bucket, key))
        r = c.fetchone()
    if not r:
        raise HTTPException(404, "no such object")
    release_chunks([x[0] for x in r[0]], kind)
    es_delete(bucket, key)
    return {"deleted": key}


@app.get("/search", dependencies=[Depends(auth)])
def search(prefix: str = "", bucket: str = None, min_size: int = None, max_size: int = None,
           after: str = None, before: str = None, tag: str = None, limit: int = 1000):
    q, a = ["key LIKE %s"], [prefix.replace("%", r"\%") + "%"]
    for cond, val in [("bucket=%s", bucket), ("size>=%s", min_size), ("size<=%s", max_size),
                      ("created>=%s", after), ("created<=%s", before)]:
        if val is not None:
            q.append(cond); a.append(val)
    if tag:
        k, _, v = tag.partition("=")
        q.append("tags->>%s = %s"); a += [k, v]
    with db() as c:
        c.execute(f"SELECT bucket,key,size,created,tags FROM objects WHERE {' AND '.join(q)} ORDER BY bucket,key LIMIT %s", a + [limit])
        return [{"bucket": r[0], "key": r[1], "size": r[2], "created": r[3].isoformat(), "tags": r[4]} for r in c.fetchall()]


@app.get("/es/search", dependencies=[Depends(auth)])
def es_search(q: str = "*"):
    if not ES_URL:
        raise HTTPException(503, "Elasticsearch not configured")
    r = http.post(f"{ES_URL}/objects/_search", timeout=5, json={
        "size": 100, "query": {"query_string": {"query": q, "fields": ["key", "bucket", "tags.*"]}}})
    if r.status_code == 404:
        return []
    return [h["_source"] for h in r.json()["hits"]["hits"]]


def degraded_chunks():
    """Chunks with fewer healthy copies/shares than the target."""
    live = alive()
    with db() as c:
        c.execute("SELECT hash, kind, loc FROM chunks WHERE loc <> 'null'")
        rows = c.fetchall()
    out = []
    for h, kind, loc in rows:
        if kind == "rep":
            if len([n for n in loc if n in live]) < N:
                out.append((h, kind, loc))
        elif len([e for e in loc["shares"] if e["node"] in live]) < K + M:
            out.append((h, kind, loc))
    return out


@app.get("/status", dependencies=[Depends(auth)])
def status():
    now = time.time()
    with db() as c:
        c.execute("SELECT kind, loc FROM chunks WHERE loc <> 'null'")
        per_node = defaultdict(int)
        for kind, loc in c.fetchall():
            for n in (loc if kind == "rep" else [e["node"] for e in loc["shares"]]):
                per_node[n] += 1
        c.execute("SELECT bucket, chunks FROM objects")
        objs = c.fetchall()
    bad = {d[0] for d in degraded_chunks()}
    degraded_objs = sum(1 for _, ch in objs if any(x[0] in bad for x in ch))
    return {
        "nodes": [{"id": n[0], "url": n[1], "status": "up" if now - n[4] < FAIL_AFTER else "down",
                   "free_bytes": n[2], "chunks_on_disk": n[3], "chunks_in_metadata": per_node.get(n[0], 0),
                   "last_heartbeat_sec_ago": round(now - n[4], 1)} for n in all_nodes()],
        "objects": len(objs), "degraded_chunks": len(bad), "degraded_objects": degraded_objs,
        "repair": repair_info(), "served_by": socket.gethostname(), "config": {"N": N, "W": W, "R": R, "K": K, "M": M},
    }


@app.get("/buckets/{bucket}/stats", dependencies=[Depends(auth)])
def bucket_stats(bucket: str):
    """Logical vs physical size: used for the replication vs EC comparison."""
    kind = bucket_mode(bucket)
    with db() as c:
        c.execute("SELECT size, chunks FROM objects WHERE bucket=%s", (bucket,))
        rows = c.fetchall()
    logical = sum(r[0] for r in rows)
    uniq = {h: s for _, ch in rows for h, s in ch}
    unique_bytes = sum(uniq.values())
    if kind == "rep":
        physical = unique_bytes * N
    else:
        physical = sum(-(-s // K) * (K + M) for s in uniq.values())
    return {"bucket": bucket, "mode": kind, "objects": len(rows), "logical_bytes": logical,
            "total_chunk_refs": sum(len(ch) for _, ch in rows), "unique_chunks": len(uniq),
            "unique_bytes": unique_bytes, "physical_bytes": physical,
            "dedup_ratio": round(logical / unique_bytes, 3) if unique_bytes else 0,
            "storage_overhead": round(physical / unique_bytes, 3) if unique_bytes else 0}


# ---------------- background: repair + scrubbing ----------------
def repair_chunk(h, kind, loc, live):
    ring = Ring(list(live))
    if kind == "rep":
        good = [n for n in loc if n in live]
        if not good:
            bump("unrecoverable")
            return
        data = node_get(live[good[0]], h)
        if data is None:
            return
        bump("repair_bytes_read", len(data))
        for t in [n for n in ring.lookup(h, len(live)) if n not in good][: N - len(good)]:
            if node_put(live[t], h, data):
                good.append(t)
                bump("repair_bytes_written", len(data))
        new = good
    else:
        good = [e for e in loc["shares"] if e["node"] in live]
        if len(good) < K:
            bump("unrecoverable")
            return
        blocks, nums = [], []
        for e in good:
            if len(blocks) == K:
                break
            b = node_get(live[e["node"]], e["h"])
            if b is not None:
                blocks.append(b); nums.append(e["i"])
                bump("repair_bytes_read", len(b))
        if len(blocks) < K:
            return
        data = easyfec.Decoder(K, K + M).decode(blocks, nums, loc["padlen"])
        shares = easyfec.Encoder(K, K + M).encode(data)
        have = {e["i"] for e in good}
        used = {e["node"] for e in good}
        order = ring.lookup(h, len(live))
        free = [n for n in order if n not in used] or order
        for j, i in enumerate(i for i in range(K + M) if i not in have):
            t = free[j % len(free)]
            sh = hashlib.sha256(shares[i]).hexdigest()
            if node_put(live[t], sh, shares[i]):
                good.append({"i": i, "h": sh, "node": t})
                bump("repair_bytes_written", len(shares[i]))
        new = {"padlen": loc["padlen"], "shares": sorted(good, key=lambda e: e["i"])}
    with db() as c:
        c.execute("UPDATE chunks SET loc=%s WHERE hash=%s AND kind=%s", (json.dumps(new), h, kind))
    bump("repaired_chunks")


def repair_loop():
    while True:
        time.sleep(REPAIR_EVERY)
        try:
            live = alive()
            todo = degraded_chunks()
            if mq.URL:  # hand repair jobs to the workers through RabbitMQ
                if todo and mq.depth() == 0:
                    mq.publish([{"type": "repair", "hash": h, "kind": k} for h, k, _ in todo])
                continue
            for h, kind, loc in todo:
                repair_chunk(h, kind, loc, live)
        except Exception as e:
            print("repair error", e)


def scrub_once():
    """Re-read every copy; nodes verify SHA-256 on read. Bad copies are dropped so repair rebuilds them."""
    live = alive()
    checked = found = 0
    with db() as c:
        c.execute("SELECT hash, kind, loc FROM chunks WHERE loc <> 'null'")
        rows = c.fetchall()
    for h, kind, loc in rows:
        items = [(n, h) for n in loc] if kind == "rep" else [(e["node"], e["h"]) for e in loc["shares"]]
        bad = []
        for n, ch in items:
            if n in live:
                checked += 1
                if node_get(live[n], ch) is None:
                    bad.append((n, ch))
                    node_delete(live[n], ch)
        if bad:
            found += len(bad)
            if kind == "rep":
                loc = [n for n in loc if (n, h) not in bad]
            else:
                loc["shares"] = [e for e in loc["shares"] if (e["node"], e["h"]) not in bad]
            with db() as c:
                c.execute("UPDATE chunks SET loc=%s WHERE hash=%s AND kind=%s", (json.dumps(loc), h, kind))
            repair_chunk(h, kind, loc, live)
    bump("scrub_checked", checked)
    bump("scrub_corrupt_found", found)
    bump("scrub_runs")


def scrub_loop():
    while True:
        time.sleep(SCRUB_EVERY)
        try:
            scrub_once()
        except Exception as e:
            print("scrub error", e)


@app.post("/admin/scrub", dependencies=[Depends(auth)])
def trigger_scrub():
    scrub_once()
    return counters()


@app.on_event("startup")
def startup():
    init_db()
    if RUN_BACKGROUND:
        threading.Thread(target=repair_loop, daemon=True).start()
        threading.Thread(target=scrub_loop, daemon=True).start()


# ---------------- rebalancing ----------------
def rebalance_chunk(h, kind, loc, live):
    """Move a replicated chunk to the nodes the ring now chooses (used when a node joins)."""
    if kind != "rep":
        return
    target = Ring(list(live)).lookup(h, N)
    have = [n for n in loc if n in live]
    if not have or set(have) == set(target):
        return
    data = node_get(live[have[0]], h)
    if data is None:
        return
    for t in target:
        if t not in have and node_put(live[t], h, data):
            bump("rebalance_bytes", len(data))
    for n in have:
        if n not in target:
            node_delete(live[n], h)
    with db() as c:
        c.execute("UPDATE chunks SET loc=%s WHERE hash=%s AND kind=%s", (json.dumps(target), h, kind))
    bump("rebalanced_chunks")


def enqueue_rebalance():
    time.sleep(5)
    with db() as c:
        c.execute("SELECT hash FROM chunks WHERE kind='rep' AND loc <> 'null'")
        hashes = [r[0] for r in c.fetchall()]
    mq.publish([{"type": "rebalance", "hash": h, "kind": "rep"} for h in hashes])
    return len(hashes)


@app.post("/admin/rebalance", dependencies=[Depends(auth)])
def trigger_rebalance():
    if not mq.URL:
        raise HTTPException(503, "RabbitMQ not configured")
    with db() as c:
        c.execute("SELECT count(*) FROM chunks WHERE kind='rep'")
    threading.Thread(target=enqueue_rebalance, daemon=True).start()
    return {"queued": "rebalance jobs for all replicated chunks"}


def repair_info():
    info = counters()
    try:
        info["repair_pending"] = mq.depth() if mq.URL else len(degraded_chunks())
    except Exception:
        info["repair_pending"] = -1
    return info


# ---------------- Prometheus metrics ----------------
REQS = Counter("dfs_requests_total", "HTTP requests", ["method", "route", "status"])
LAT = Histogram("dfs_request_seconds", "HTTP request latency", ["method", "route"])
BYTES_UP = Counter("dfs_bytes_uploaded_total", "Bytes uploaded")
BYTES_DOWN = Counter("dfs_bytes_downloaded_total", "Bytes downloaded")
G_FREE = Gauge("dfs_node_free_bytes", "Free disk per node", ["node"])
G_CHUNKS = Gauge("dfs_node_chunks", "Chunks stored per node", ["node"])
G_UP = Gauge("dfs_node_up", "Node alive (1/0)", ["node"])
G_DEG = Gauge("dfs_degraded_objects", "Degraded objects")
G_DEGC = Gauge("dfs_degraded_chunks", "Degraded chunks")
G_CNT = Gauge("dfs_counter", "Repair/scrub/job counters (cumulative)", ["name"])
G_Q = Gauge("dfs_queue_depth", "Jobs waiting in RabbitMQ")


@app.middleware("http")
async def metrics_mw(request, call_next):
    t = time.time()
    resp = await call_next(request)
    route = getattr(request.scope.get("route"), "path", "other")
    if route != "/metrics":
        LAT.labels(request.method, route).observe(time.time() - t)
        REQS.labels(request.method, route, resp.status_code).inc()
    resp.headers["X-Served-By"] = socket.gethostname()
    return resp


@app.get("/metrics")
def metrics():
    now = time.time()
    for n in all_nodes():
        G_FREE.labels(n[0]).set(n[2]); G_CHUNKS.labels(n[0]).set(n[3]); G_UP.labels(n[0]).set(int(now - n[4] < FAIL_AFTER))
    st = status()
    G_DEG.set(st["degraded_objects"]); G_DEGC.set(st["degraded_chunks"])
    for k, v in counters().items():
        G_CNT.labels(k).set(v)
    if mq.URL:
        try:
            G_Q.set(mq.depth())
        except Exception:
            pass
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)
