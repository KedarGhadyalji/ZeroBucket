#!/usr/bin/env bash
# Cross-language conformance: Node <-> Python on ONE database (and, optionally, ONE S3 bucket).
# Needs ZEROBUCKET_TEST_DATABASE_URL, `pip install zerobucket`, and `npm install` in packages/typescript.
# Set ZEROBUCKET_TEST_S3_ENDPOINT (e.g. a moto/MinIO server) to also test tiering interop.
# Run from packages/typescript.
set -euo pipefail
D=$(mktemp -d)
if [ -n "${ZEROBUCKET_TEST_S3_ENDPOINT:-}" ]; then
  export ZEROBUCKET_TEST_S3_BUCKET="zb-conformance-$$"
  python3 -c "import boto3,os; boto3.client('s3', endpoint_url=os.environ['ZEROBUCKET_TEST_S3_ENDPOINT'], region_name='us-east-1').create_bucket(Bucket=os.environ['ZEROBUCKET_TEST_S3_BUCKET'])"
fi
npx tsx conformance/node_side.ts write "$D"
python3 conformance/py_side.py read "$D"
python3 conformance/py_side.py write "$D"   # also compares validation verdicts
npx tsx conformance/node_side.ts read "$D"
echo "CONFORMANCE PASSED"