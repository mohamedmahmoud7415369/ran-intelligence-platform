#!/usr/bin/env bash
set -euo pipefail

stack_name="${1:-ran}"
connect_url="${CONNECT_URL:-http://127.0.0.1:8083}"

echo "=== Swarm nodes ==="
docker node ls

echo "=== Stack services ==="
docker stack services "$stack_name"

echo "=== Kafka topic initialization ==="
docker service logs --raw "${stack_name}_kafka-init" 2>/dev/null || true

echo "=== HDFS initialization test ==="
docker service logs --raw "${stack_name}_hdfs-smoke-test" 2>/dev/null || true

echo "=== Kafka Connect workers/connectors ==="
curl -fsS "$connect_url/connectors" || true
echo
curl -fsS "$connect_url/connectors/ran-hdfs3-sink/status" || true
echo

