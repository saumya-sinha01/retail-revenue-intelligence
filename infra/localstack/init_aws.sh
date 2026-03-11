#!/usr/bin/env bash
set -euo pipefail

ENDPOINT_URL="${LOCALSTACK_ENDPOINT:-http://localhost:4566}"

echo "Creating S3 buckets in LocalStack at ${ENDPOINT_URL}..."

aws --endpoint-url="$ENDPOINT_URL" s3 mb s3://retail-bronze || true
aws --endpoint-url="$ENDPOINT_URL" s3 mb s3://retail-silver || true
aws --endpoint-url="$ENDPOINT_URL" s3 mb s3://retail-gold || true

echo "Buckets ready: retail-bronze, retail-silver, retail-gold"