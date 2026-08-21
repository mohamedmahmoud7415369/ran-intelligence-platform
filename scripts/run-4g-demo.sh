#!/usr/bin/env bash
set -Eeuo pipefail

STACK_NAME="${STACK_NAME:-ran-platform}"
TOPIC="${DEMO_TOPIC:-ran-4g-demo}"
CONNECTOR="${DEMO_CONNECTOR:-ran-4g-hdfs-sink}"
PRODUCER_CONTAINER="${PRODUCER_CONTAINER:-ran-producer-4g}"
PRODUCER_IMAGE="${PRODUCER_IMAGE:-ran-producer:1.0}"
SOURCE_FILE="${SOURCE_FILE:-Dataset_01_LTE_2100.csv}"
SEND_DELAY_SECONDS="${SEND_DELAY_SECONDS:-0.005}"
HDFS_TOPIC_PATH="/ran-4g-demo-data/${TOPIC}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
DATA_DIR="${PROJECT_ROOT}/data/Raw"
SOURCE_PATH="${DATA_DIR}/${SOURCE_FILE}"

service_container() {
  local service_name="$1"
  docker ps -q \
    --filter "label=com.docker.swarm.service.name=${STACK_NAME}_${service_name}" \
    | head -n1
}

fail() {
  echo "ERROR: $*" >&2
  exit 1
}

[[ -f "$SOURCE_PATH" ]] || fail "CSV file not found: ${SOURCE_PATH}"
docker image inspect "$PRODUCER_IMAGE" >/dev/null 2>&1 \
  || fail "Docker image not found: ${PRODUCER_IMAGE}"

EXPECTED_RECORDS="$(( $(wc -l < "$SOURCE_PATH") - 1 ))"

KAFKA_CONTAINER="$(service_container kafka2)"
CONNECT_CONTAINER="$(service_container connect2)"
NN_CONTAINER="$(service_container namenode2)"

[[ -n "$KAFKA_CONTAINER" ]] || fail "Kafka2 is not running on this node."
[[ -n "$CONNECT_CONTAINER" ]] || fail "Connect2 is not running on this node."
[[ -n "$NN_CONTAINER" ]] || fail "NameNode2 is not running on this node."

if docker exec "$KAFKA_CONTAINER" \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka2:9092 \
  --list | grep -qx "$TOPIC"; then

  EXISTING_RECORDS="$(docker exec "$KAFKA_CONTAINER" \
    /opt/kafka/bin/kafka-get-offsets.sh \
    --bootstrap-server kafka2:9092 \
    --topic "$TOPIC" 2>/dev/null \
    | awk -F: '{sum += $3} END {print sum+0}')"

  if (( EXISTING_RECORDS > 0 )); then
    fail "Topic ${TOPIC} already contains ${EXISTING_RECORDS} records. Run cleanup-4g-demo.sh first."
  fi
else
  docker exec "$KAFKA_CONTAINER" \
    /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server kafka2:9092 \
    --create \
    --topic "$TOPIC" \
    --partitions 3 \
    --replication-factor 3 \
    --config min.insync.replicas=2
fi

docker exec -i "$CONNECT_CONTAINER" \
  curl -fsS \
  -X PUT \
  -H 'Content-Type: application/json' \
  --data-binary @- \
  "http://localhost:8083/connectors/${CONNECTOR}/config" \
  >/dev/null <<JSON
{
  "connector.class": "io.confluent.connect.hdfs3.Hdfs3SinkConnector",
  "tasks.max": "2",
  "topics": "${TOPIC}",
  "store.url": "hdfs://ran-ha",
  "hadoop.conf.dir": "/etc/hadoop",
  "format.class": "io.confluent.connect.hdfs3.json.JsonFormat",
  "storage.class": "io.confluent.connect.hdfs3.storage.HdfsStorage",
  "partitioner.class": "io.confluent.connect.storage.partitioner.DefaultPartitioner",
  "key.converter": "org.apache.kafka.connect.storage.StringConverter",
  "value.converter": "org.apache.kafka.connect.json.JsonConverter",
  "value.converter.schemas.enable": "false",
  "topics.dir": "ran-4g-demo-data",
  "logs.dir": "ran-4g-demo-logs",
  "flush.size": "1000",
  "rotate.interval.ms": "60000",
  "schema.compatibility": "NONE",
  "confluent.topic.bootstrap.servers": "kafka1:9092,kafka2:9092,kafka3:9092",
  "confluent.topic.replication.factor": "3",
  "errors.tolerance": "all",
  "errors.deadletterqueue.topic.name": "ran-telemetry-dlq",
  "errors.deadletterqueue.topic.replication.factor": "3",
  "errors.log.enable": "true"
}
JSON

echo "Waiting for connector ${CONNECTOR}..."
for _ in $(seq 1 24); do
  CONNECTOR_STATE="$(docker exec "$CONNECT_CONTAINER" \
    curl -fsS \
    "http://localhost:8083/connectors/${CONNECTOR}/status" 2>/dev/null \
    | sed -n 's/.*"connector":{"state":"\([^"]*\)".*/\1/p' || true)"

  [[ "$CONNECTOR_STATE" == "RUNNING" ]] && break
  sleep 5
done

[[ "${CONNECTOR_STATE:-}" == "RUNNING" ]] \
  || fail "Connector did not reach RUNNING state."

docker rm -f "$PRODUCER_CONTAINER" >/dev/null 2>&1 || true

echo "Running ${SOURCE_FILE} (${EXPECTED_RECORDS} records)..."
docker run -d \
  --name "$PRODUCER_CONTAINER" \
  --log-opt max-size=10m \
  --log-opt max-file=2 \
  --network "${STACK_NAME}_ran-network" \
  -v "${DATA_DIR}:/data/Raw:ro" \
  -e DATA_DIR=/data/Raw \
  -e SOURCE_FILE="$SOURCE_FILE" \
  -e KAFKA_BOOTSTRAP_SERVERS=kafka1:9092,kafka2:9092,kafka3:9092 \
  -e KAFKA_TOPIC="$TOPIC" \
  -e MAX_EVENTS=0 \
  -e SEND_DELAY_SECONDS="$SEND_DELAY_SECONDS" \
  "$PRODUCER_IMAGE" >/dev/null

while [[ "$(docker inspect --format '{{.State.Running}}' "$PRODUCER_CONTAINER" 2>/dev/null)" == "true" ]]; do
  TOTAL="$(docker exec "$KAFKA_CONTAINER" \
    /opt/kafka/bin/kafka-get-offsets.sh \
    --bootstrap-server kafka2:9092 \
    --topic "$TOPIC" 2>/dev/null \
    | awk -F: '{sum += $3} END {print sum+0}')"

  echo "Kafka progress: ${TOTAL} / ${EXPECTED_RECORDS}"
  sleep 10
done

PRODUCER_EXIT="$(docker inspect --format '{{.State.ExitCode}}' "$PRODUCER_CONTAINER")"
FINAL_KAFKA_COUNT="$(docker exec "$KAFKA_CONTAINER" \
  /opt/kafka/bin/kafka-get-offsets.sh \
  --bootstrap-server kafka2:9092 \
  --topic "$TOPIC" 2>/dev/null \
  | awk -F: '{sum += $3} END {print sum+0}')"

echo "Producer exit code: ${PRODUCER_EXIT}"
echo "Kafka records: ${FINAL_KAFKA_COUNT} / ${EXPECTED_RECORDS}"

if [[ "$PRODUCER_EXIT" != "0" || "$FINAL_KAFKA_COUNT" != "$EXPECTED_RECORDS" ]]; then
  docker logs --tail 30 "$PRODUCER_CONTAINER"
  fail "Producer run was incomplete. Clean the demo before retrying."
fi

echo "Waiting for Kafka Connect lag to reach zero..."
for _ in $(seq 1 60); do
  LAG="$(docker exec "$KAFKA_CONTAINER" \
    /opt/kafka/bin/kafka-consumer-groups.sh \
    --bootstrap-server kafka2:9092 \
    --group "connect-${CONNECTOR}" \
    --describe 2>/dev/null \
    | awk -v topic="$TOPIC" '$2 == topic {sum += $6; found=1} END {if (found) print sum; else print -1}')"

  echo "Connector lag: ${LAG}"
  [[ "$LAG" == "0" ]] && break
  sleep 5
done

[[ "${LAG:-}" == "0" ]] || fail "Connector lag did not reach zero."

echo "Waiting for the final HDFS file rotation..."
for _ in $(seq 1 36); do
  HDFS_RECORDS="$(docker exec "$NN_CONTAINER" bash -lc \
    "hdfs dfs -cat '${HDFS_TOPIC_PATH}/partition=*/*.json' 2>/dev/null | wc -l")"

  echo "HDFS records: ${HDFS_RECORDS} / ${EXPECTED_RECORDS}"
  [[ "$HDFS_RECORDS" == "$EXPECTED_RECORDS" ]] && break
  sleep 5
done

[[ "${HDFS_RECORDS:-}" == "$EXPECTED_RECORDS" ]] \
  || fail "HDFS record count does not match the CSV file."

echo
docker exec "$CONNECT_CONTAINER" \
  curl -fsS "http://localhost:8083/connectors/${CONNECTOR}/status"
echo

docker exec "$NN_CONTAINER" hdfs dfs -ls -R "$HDFS_TOPIC_PATH"

docker exec "$NN_CONTAINER" \
  hdfs fsck "$HDFS_TOPIC_PATH" -files -blocks -locations \
  | tail -n 35

echo
echo "4G demo completed successfully: ${EXPECTED_RECORDS} records stored in HDFS."

