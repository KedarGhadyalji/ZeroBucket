#!/usr/bin/env bash
# Cross-language conformance: Node <-> Python on ONE database (and, optionally, ONE S3 bucket).
# Runs the full flow on Postgres (classic + tiering interop if an S3 endpoint is set, then dedup) and on a shared SQLite file (same two modes).
# Needs ZEROBUCKET_TEST_DATABASE_URL, `pip install zerobucket[s3]`, and `npm install` in packages/typescript.
# Set ZEROBUCKET_TEST_S3_ENDPOINT (e.g. a moto/MinIO server) to also test tiering interop.
# Run from packages/typescript.
set -euo pipefail

run_flow() {
  local D; D=$(mktemp -d)
  npx tsx conformance/node_side.ts write "$D"
  python3 conformance/py_side.py read "$D"
  python3 conformance/py_side.py write "$D"   # also compares validation verdicts
  npx tsx conformance/node_side.ts read "$D"
}

if [ -n "${ZEROBUCKET_TEST_S3_ENDPOINT:-}" ]; then
  export ZEROBUCKET_TEST_S3_BUCKET="zb-conformance-$$"
  python3 -c "import boto3,os; boto3.client('s3', endpoint_url=os.environ['ZEROBUCKET_TEST_S3_ENDPOINT'], region_name='us-east-1').create_bucket(Bucket=os.environ['ZEROBUCKET_TEST_S3_BUCKET'])"
fi

echo "=== classic mode ==="
run_flow
echo "=== dedup mode ==="
ZEROBUCKET_CONF_DEDUP=1 run_flow
# SQLite: Node and Python share one real .db FILE (also tiering interop in classic mode, via the same S3 bucket).
SQ=$(mktemp -d)
echo "=== sqlite: classic mode ==="
ZEROBUCKET_CONF_BACKEND=sqlite ZEROBUCKET_CONF_SQLITE_PATH="$SQ/classic.db" run_flow
echo "=== sqlite: dedup mode ==="
ZEROBUCKET_CONF_BACKEND=sqlite ZEROBUCKET_CONF_SQLITE_PATH="$SQ/dedup.db" ZEROBUCKET_CONF_DEDUP=1 run_flow
echo "CONFORMANCE PASSED"