#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/00_common.sh"

section "05 - Bus de eventos Kafka (raw y filtrados)"
ensure_service kafka || exit 0

section "Topics disponibles"
run_service kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --list | sort

consume_one() {
  local topic="$1"
  section "1 mensaje de ${topic}"
  run_service kafka bash -lc "timeout 10 /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic ${topic} --from-beginning --max-messages 1" || true
}

consume_one transport.raw
consume_one transport.filtered
consume_one transport.weather.raw
consume_one transport.weather.filtered
