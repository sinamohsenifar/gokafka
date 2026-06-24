#!/usr/bin/env bash
set -euo pipefail

BOOTSTRAP="${KAFKA_BOOTSTRAP:-kafka:29092}"
USER="${SCRAM_USER:-gokafka}"
PASS="${SCRAM_PASSWORD:-gokafka-secret}"

echo "Waiting for Kafka at $BOOTSTRAP..."
ready=0
for i in $(seq 1 90); do
  if /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server "$BOOTSTRAP" >/dev/null 2>&1; then
    ready=1
    break
  fi
  sleep 2
done
if [ "$ready" -ne 1 ]; then
  echo "Kafka not reachable at $BOOTSTRAP after 180s"
  exit 1
fi

echo "Creating SCRAM credentials for user $USER..."
/opt/kafka/bin/kafka-configs.sh --bootstrap-server "$BOOTSTRAP" \
  --alter --add-config "SCRAM-SHA-256=[password=${PASS}]" \
  --entity-type users --entity-name "$USER"

/opt/kafka/bin/kafka-configs.sh --bootstrap-server "$BOOTSTRAP" \
  --alter --add-config "SCRAM-SHA-512=[password=${PASS}]" \
  --entity-type users --entity-name "$USER"

echo "SCRAM users ready."

if [ -x /opt/kafka/bin/kafka-features.sh ]; then
  echo "Enabling share groups (share.version=1) when supported..."
  /opt/kafka/bin/kafka-features.sh --bootstrap-server "$BOOTSTRAP" upgrade --feature share.version=1 || true
fi
