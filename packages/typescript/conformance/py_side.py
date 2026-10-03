"""Cross-language conformance, Python half. Usage: py_side.py read|write <dir>"""

import hashlib, json, os, sys
from zerobucket import ZeroBucket
from zerobucket.validation import validate_image

mode, d = sys.argv[1], sys.argv[2]
zb = ZeroBucket(database_url=os.environ["ZEROBUCKET_TEST_DATABASE_URL"])
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
