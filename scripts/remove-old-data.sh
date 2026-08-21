#!/usr/bin/env bash
set -euo pipefail

stack_name="${1:-ran}"

if docker stack services "$stack_name" >/dev/null 2>&1; then
  echo "Stack '$stack_name' still exists. Remove it from a manager first:"
  echo "  docker stack rm $stack_name"
  exit 1
fi

echo "Host: $(hostname)"
echo "This deletes the local Docker volumes for stack '$stack_name'."
echo "Run this script separately on Victus, zzzz-vm and abdullah-VMware-Virtual-Platform."
read -r -p "Type DELETE-$stack_name to continue: " confirmation
if [[ "$confirmation" != "DELETE-$stack_name" ]]; then
  echo "Cancelled."
  exit 1
fi

volume_suffixes=(
  zookeeper1-data zookeeper1-log zookeeper2-data zookeeper2-log zookeeper3-data zookeeper3-log
  kafka1-data kafka2-data kafka3-data
  journalnode1-data journalnode2-data journalnode3-data
  namenode1-data namenode2-data
  datanode1-data datanode2-data datanode3-data
  hdfs-zkfc-state
  clickhouse-data grafana-data prometheus-data
)

for suffix in "${volume_suffixes[@]}"; do
  volume_name="${stack_name}_${suffix}"
  if docker volume inspect "$volume_name" >/dev/null 2>&1; then
    echo "Removing $volume_name"
    docker volume rm "$volume_name"
  fi
done

echo "Local stack volumes removed from $(hostname)."

