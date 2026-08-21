#!/usr/bin/env bash
set -euo pipefail

connect_url="${CONNECT_URL:-http://127.0.0.1:8083}"
config_file="${1:-connect/connector-hdfs3.json}"

if [[ ! -f "$config_file" ]]; then
  echo "Connector config not found: $config_file"
  exit 1
fi

echo "Waiting for Kafka Connect at $connect_url"
for _ in $(seq 1 60); do
  if curl -fsS "$connect_url/connectors" >/dev/null; then
    break
  fi
  sleep 5
done

curl -fsS "$connect_url/connectors" >/dev/null

echo "Creating or updating ran-hdfs3-sink"
curl -fsS -X PUT \
  -H 'Content-Type: application/json' \
  --data-binary "@$config_file" \
  "$connect_url/connectors/ran-hdfs3-sink/config"
echo

echo "Connector status:"
curl -fsS "$connect_url/connectors/ran-hdfs3-sink/status"
echo

