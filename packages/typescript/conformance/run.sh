#!/usr/bin/env bash
# Cross-language conformance: Node <-> Python on ONE database. Needs ZEROBUCKET_TEST_DATABASE_URL,
# `pip install zerobucket`, and `npm install` in packages/typescript. Run from packages/typescript.
set -euo pipefail
D=$(mktemp -d)
npx tsx conformance/node_side.ts write "$D"
python3 conformance/py_side.py read "$D"
python3 conformance/py_side.py write "$D"   # also compares validation verdicts
npx tsx conformance/node_side.ts read "$D"
echo "CONFORMANCE PASSED"