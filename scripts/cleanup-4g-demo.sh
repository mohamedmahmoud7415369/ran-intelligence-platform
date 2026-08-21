#!/usr/bin/env bash
set -Eeuo pipefail

STACK_NAME="${STACK_NAME:-ran-platform}"
TOPIC="${DEMO_TOPIC:-ran-4g-demo}"
CONNECTOR="${DEMO_CONNECTOR:-ran-4g-hdfs-sink}"
PRODUCER_CONTAINER="${PRODUCER_CONTAINER:-ran-producer-4g}"
HDFS_DATA_PATH="${HDFS_DATA_PATH:-/ran-4g-demo-data}"
HDFS_LOG_PATH="${HDFS_LOG_PATH:-/ran-4g-demo-logs}"

service_container() {
  local service_name="$1"
  docker ps -q \
    --filter "label=com.docker.swarm.service.name=${STACK_NAME}_${service_name}" \
    | head -n1
}

echo "This deletes only the 4G demo resources:"
echo "  Producer container : ${PRODUCER_CONTAINER}"
echo "  Connect connector  : ${CONNECTOR}"
echo "  Kafka topic        : ${TOPIC}"
echo "  HDFS paths         : ${HDFS_DATA_PATH}, ${HDFS_LOG_PATH}"
echo
echo "The Docker stack and its persistent volumes will NOT be deleted."

if [[ "${1:-}" != "--yes" ]]; then
  read -r -p "Type DELETE-${TOPIC} to continue: " confirmation
  if [[ "$confirmation" != "DELETE-${TOPIC}" ]]; then
    echo "Cancelled."
    exit 1
  fi
fi

docker rm -f "$PRODUCER_CONTAINER" >/dev/null 2>&1 || true

CONNECT_CONTAINER="$(service_container connect2)"
if [[ -z "$CONNECT_CONTAINER" ]]; then
  CONNECT_CONTAINER="$(service_container connect1)"
fi

if [[ -n "$CONNECT_CONTAINER" ]]; then
  HTTP_CODE="$(docker exec "$CONNECT_CONTAINER" \
    curl -sS -o /dev/null -w '%{http_code}' \
    "http://localhost:8083/connectors/${CONNECTOR}" || true)"

  if [[ "$HTTP_CODE" == "200" ]]; then
    docker exec "$CONNECT_CONTAINER" \
      curl -sS -X PUT \
      "http://localhost:8083/connectors/${CONNECTOR}/stop" \
      >/dev/null || true

    docker exec "$CONNECT_CONTAINER" \
      curl -sS -X DELETE \
      "http://localhost:8083/connectors/${CONNECTOR}/offsets" \
      >/dev/null || true

    docker exec "$CONNECT_CONTAINER" \
      curl -sS -X DELETE \
      "http://localhost:8083/connectors/${CONNECTOR}" \
      >/dev/null || true

    for _ in $(seq 1 20); do
      HTTP_CODE="$(docker exec "$CONNECT_CONTAINER" \
        curl -sS -o /dev/null -w '%{http_code}' \
        "http://localhost:8083/connectors/${CONNECTOR}" || true)"
      [[ "$HTTP_CODE" == "404" ]] && break
      sleep 2
    done
  fi
else
  echo "Warning: no local Kafka Connect container was found."
fi

NN_CONTAINER="$(service_container namenode2)"
if [[ -z "$NN_CONTAINER" ]]; then
  NN_CONTAINER="$(service_container namenode1)"
fi

if [[ -n "$NN_CONTAINER" ]]; then
  for hdfs_path in "$HDFS_DATA_PATH" "$HDFS_LOG_PATH"; do
    if docker exec "$NN_CONTAINER" hdfs dfs -test -e "$hdfs_path"; then
      docker exec "$NN_CONTAINER" \
        hdfs dfs -rm -r -skipTrash "$hdfs_path"
    fi
  done
else
  echo "Warning: no local NameNode container was found."
fi

KAFKA_CONTAINER="$(service_container kafka2)"
if [[ -z "$KAFKA_CONTAINER" ]]; then
  KAFKA_CONTAINER="$(service_container kafka1)"
fi

if [[ -n "$KAFKA_CONTAINER" ]]; then
  if docker exec "$KAFKA_CONTAINER" \
    /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server kafka1:9092,kafka2:9092,kafka3:9092 \
    --list | grep -qx "$TOPIC"; then

    docker exec "$KAFKA_CONTAINER" \
      /opt/kafka/bin/kafka-topics.sh \
      --bootstrap-server kafka1:9092,kafka2:9092,kafka3:9092 \
      --delete \
      --topic "$TOPIC"

    for _ in $(seq 1 30); do
      if ! docker exec "$KAFKA_CONTAINER" \
        /opt/kafka/bin/kafka-topics.sh \
        --bootstrap-server kafka1:9092,kafka2:9092,kafka3:9092 \
        --list | grep -qx "$TOPIC"; then
        break
      fi
      sleep 2
    done
  fi
else
  echo "Warning: no local Kafka container was found."
fi

echo
echo "4G demo cleanup completed."
