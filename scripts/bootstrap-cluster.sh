#!/usr/bin/env bash
set -euo pipefail

stack_name="${1:-ran}"

service_exists() {
  docker service inspect "${stack_name}_$1" >/dev/null 2>&1
}

wait_replicas() {
  local short_name="$1"
  local expected="${2:-1/1}"
  local service_name="${stack_name}_${short_name}"
  local deadline=$((SECONDS + 600))

  echo "Waiting for ${service_name} => ${expected}"
  while (( SECONDS < deadline )); do
    current="$(docker service ls --filter "name=^${service_name}$" --format '{{.Replicas}}' | head -n1)"
    if [[ "$current" == "$expected" ]]; then
      return 0
    fi
    sleep 5
  done

  docker service ps --no-trunc "$service_name" || true
  echo "Timeout waiting for $service_name"
  exit 1
}

run_job() {
  local short_name="$1"
  local service_name="${stack_name}_${short_name}"
  local deadline=$((SECONDS + 900))

  echo "Running one-time job: $service_name"
  docker service scale "${service_name}=0" >/dev/null
  docker service scale "${service_name}=1" >/dev/null

  while (( SECONDS < deadline )); do
    state="$(docker service ps --no-trunc --format '{{.CurrentState}}' "$service_name" | head -n1)"
    case "$state" in
      Complete*)
        docker service logs --raw "$service_name" || true
        docker service scale "${service_name}=0" >/dev/null
        return 0
        ;;
      Failed*|Rejected*)
        docker service ps --no-trunc "$service_name" || true
        docker service logs --raw "$service_name" || true
        exit 1
        ;;
    esac
    sleep 5
  done

  docker service ps --no-trunc "$service_name" || true
  docker service logs --raw "$service_name" || true
  echo "Timeout running $service_name"
  exit 1
}

for required in zookeeper1 zookeeper2 zookeeper3 journalnode1 journalnode2 journalnode3 hdfs-format-nn1 hdfs-bootstrap-nn2 hdfs-format-zk hdfs-smoke-test kafka-init; do
  if ! service_exists "$required"; then
    echo "Missing service: ${stack_name}_${required}. Deploy docker-stack.yml first."
    exit 1
  fi
done

for service in zookeeper1 zookeeper2 zookeeper3 journalnode1 journalnode2 journalnode3; do
  wait_replicas "$service"
done

run_job hdfs-format-nn1
wait_replicas namenode1

run_job hdfs-bootstrap-nn2
wait_replicas namenode2

run_job hdfs-format-zk
wait_replicas zkfc1
wait_replicas zkfc2

run_job hdfs-smoke-test

for service in kafka1 kafka2 kafka3; do
  wait_replicas "$service"
done
run_job kafka-init

echo "Kafka and HDFS initialization completed."

