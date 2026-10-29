import os, time, uuid
import requests

URL = os.environ.get("GATEWAY_URL", "http://localhost:8080")
H = {"X-API-Key": "admin-key"}
B = "pytest-" + uuid.uuid4().hex[:6]
DATA = os.urandom(5 * 1024 * 1024 + 123)  # > 1 chunk


def test_auth_required():
    assert requests.get(f"{URL}/buckets").status_code == 401
    assert requests.get(f"{URL}/buckets", headers={"X-API-Key": "wrong"}).status_code == 401


def test_create_and_list_buckets():
    assert requests.post(f"{URL}/buckets/{B}?mode=rep", headers=H).status_code == 200
    assert requests.post(f"{URL}/buckets/{B}-ec?mode=ec", headers=H).status_code == 200
    assert requests.post(f"{URL}/buckets/{B}", headers=H).status_code == 409
    names = [b["name"] for b in requests.get(f"{URL}/buckets", headers=H).json()]
    assert B in names and B + "-ec" in names


def test_upload_download_range_meta():
    for b in (B, B + "-ec"):
        r = requests.put(f"{URL}/buckets/{b}/objects/docs/a.bin?tags=type=test", headers=H, data=DATA)
        assert r.status_code == 200 and r.json()["chunks"] == 2
        assert requests.get(f"{URL}/buckets/{b}/objects/docs/a.bin", headers=H).content == DATA
        r = requests.get(f"{URL}/buckets/{b}/objects/docs/a.bin", headers={**H, "Range": "bytes=4194000-4195000"})
        assert r.status_code == 206 and r.content == DATA[4194000:4195001]
        m = requests.get(f"{URL}/buckets/{b}/meta/docs/a.bin", headers=H).json()
        assert m["size"] == len(DATA) and m["version"] == 1 and m["tags"] == {"type": "test"}


def test_dedup_and_versioning():
    r = requests.put(f"{URL}/buckets/{B}/objects/docs/copy.bin", headers=H, data=DATA).json()
    assert r["new_chunks"] == 0  # identical chunks stored once
    r = requests.put(f"{URL}/buckets/{B}/objects/docs/copy.bin", headers=H, data=DATA).json()
    assert r["version"] == 2


def test_list_and_search():
    keys = [o["key"] for o in requests.get(f"{URL}/buckets/{B}/objects?prefix=docs/", headers=H).json()]
    assert keys == ["docs/a.bin", "docs/copy.bin"]
    res = requests.get(f"{URL}/search", headers=H, params={"bucket": B, "prefix": "docs/", "min_size": 1000, "tag": "type=test"}).json()
    assert [x["key"] for x in res] == ["docs/a.bin"]


def test_status():
    s = requests.get(f"{URL}/status", headers=H).json()
    assert len(s["nodes"]) >= 4 and "degraded_objects" in s


def test_delete():
    assert requests.delete(f"{URL}/buckets/{B}/objects/docs/copy.bin", headers=H).status_code == 200
    assert requests.get(f"{URL}/buckets/{B}/objects/docs/copy.bin", headers=H).status_code == 404
    assert requests.get(f"{URL}/buckets/{B}/objects/docs/a.bin", headers=H).content == DATA  # shared chunks kept


def test_rate_limit():
    codes = [requests.get(f"{URL}/buckets", headers={"X-API-Key": "test-key"}).status_code for _ in range(25)]
    assert 429 in codes
