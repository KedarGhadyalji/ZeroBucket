"""Cross-language conformance, Python half. Usage: py_side.py read|write <dir>"""

import hashlib, json, os, sys
import psycopg
from zerobucket import ZeroBucket, ObjectStorage
from zerobucket.validation import validate_image

mode, d = sys.argv[1], sys.argv[2]
DEDUP = os.environ.get("ZEROBUCKET_CONF_DEDUP") == "1"
bucket, endpoint = (
    (None, None)
    if DEDUP
    else (
        os.environ.get("ZEROBUCKET_TEST_S3_BUCKET"),
        os.environ.get("ZEROBUCKET_TEST_S3_ENDPOINT"),
    )
)
store = (
    ObjectStorage(bucket, endpoint_url=endpoint, region_name="us-east-1")
    if bucket and endpoint
    else None
)
zb = ZeroBucket(
    database_url=os.environ["ZEROBUCKET_TEST_DATABASE_URL"],
    object_storage=store,
    dedup=DEDUP,
)


def ref_count(checksum):
    with psycopg.connect(os.environ["ZEROBUCKET_TEST_DATABASE_URL"]) as c:
        row = c.execute(
            "SELECT ref_count FROM zerobucket_blobs WHERE checksum_sha256=%s",
            (checksum,),
        ).fetchone()
        return row[0] if row else None


sha = lambda b: hashlib.sha256(b).hexdigest()

if mode == "read":  # rows written by Node
    node = json.load(open(f"{d}/node_ids.json"))
    ok = 0
    for name, e in node.items():
        img = zb.get(e["id"])
        bad = []
        if sha(img.data) != e["checksum"]:
            bad.append("bytes")
        if img.checksum_sha256 != e["checksum"]:
            bad.append("stored checksum")
        if img.mime_type != e["mime"]:
            bad.append(f"mime {img.mime_type}!={e['mime']}")
        if (img.width, img.height) != (e["w"], e["h"]):
            bad.append("dims")
        if img.filename != name:
            bad.append("filename")
        if sha(b"".join(zb.get_stream(e["id"], chunk_size=1000))) != e["checksum"]:
            bad.append("stream")
        if bad:
            print("FAIL", name, bad)
            sys.exit(1)
        ok += 1
    print(f"python: read {ok}/{len(node)} Node-written rows OK")
    if DEDUP:
        a, b = (
            node["photo.jpg"],
            node["photo_copy.jpg"],
        )  # Node wrote both: one shared blob
        before = ref_count(a["checksum"])
        assert zb.delete(b["id"]) is True
        after = ref_count(a["checksum"])
        assert before is not None and after == before - 1, (before, after)
        assert sha(zb.get(a["id"]).data) == a["checksum"], "surviving ref unreadable"
        print(
            f"python: deleted a Node-written ref to a shared blob (ref_count {before} -> {after}); the other ref still reads fine"
        )
    if store:
        # Python must be able to clean up an object NODE tiered.
        t = next((e for e in node.values() if e.get("tiered")), None)
        if not t:
            print("FAIL: Node tiered nothing")
            sys.exit(1)
        assert store.exists(t["id"]), "node-tiered object missing from bucket"
        assert zb.delete(t["id"]) is True
        assert not store.exists(t["id"]), "python delete left node-tiered object behind"
        print("python: deleted a Node-tiered row and its S3 object was cleaned up")
else:  # validate the same corpus + write rows for Node to read
    verdicts, stored = {}, {}
    for name in sorted(os.listdir(f"{d}/corpus")):
        data = open(f"{d}/corpus/{name}", "rb").read()
        try:
            v = validate_image(data, max_bytes=8 * 1024 * 1024)
            verdicts[name] = {
                "ok": True,
                "mime": v.mime_type,
                "w": v.width,
                "h": v.height,
            }
            stored[name] = {
                "id": zb.put(data, filename=name),
                "checksum": sha(data),
                "mime": v.mime_type,
                "w": v.width,
                "h": v.height,
            }
        except Exception:
            verdicts[name] = {"ok": False}
    if store:
        for name in ("rgb.png", "photo.jpg"):
            if zb.tier_to_object_storage(stored[name]["id"]):
                stored[name]["tiered"] = True
    json.dump(stored, open(f"{d}/py_ids.json", "w"))
    node = json.load(open(f"{d}/node_validation.json"))
    diffs = {k: (node[k], verdicts[k]) for k in node if node[k] != verdicts[k]}
    print(
        f"python: validated {len(verdicts)} files; verdict differences vs Node: {len(diffs)}"
    )
    for k, (n, p) in diffs.items():
        print("  DIFF", k, "node=", n, "python=", p)
    sys.exit(1 if diffs else 0)
zb.close()
