#!/usr/bin/env sh
# Publish a progress update straight onto the broker, bypassing the worker, using RabbitMQ's own
# CLI (rabbitmqadmin, inside the rabbitmq container). Run from the repo root with the stack up.
#
#   scripts/publish_update.sh <task_id> <in_progress|done|failed> [message_id]
#       A well-formed update. Re-use a message_id to simulate a duplicate delivery.
#   scripts/publish_update.sh --raw '<payload>'
#       Anything at all (e.g. 'garbage'), to simulate a poison message.
#
# Then watch `make logs`, GET the task/tenant, and check the DLQ with `make queues`.
set -eu

USER_NAME=${RABBITMQ_USER:-control_plane}
PASSWORD=${RABBITMQ_PASSWORD:-control_plane}

publish() { # <routing_key> <payload> <message_id>
  docker compose exec -T rabbitmq rabbitmqadmin -u "$USER_NAME" -p "$PASSWORD" \
    publish message --exchange task-updates --routing-key "$1" --payload "$2" \
    --properties "{\"content_type\": \"application/json\", \"delivery_mode\": 2, \"message_id\": \"$3\"}"
}

new_uuid() { docker compose exec -T rabbitmq cat /proc/sys/kernel/random/uuid; }

if [ "${1:-}" = "--raw" ]; then
  [ $# -eq 2 ] || { echo "usage: $0 --raw '<payload>'" >&2; exit 2; }
  publish "task-update.raw" "$2" "raw-$(new_uuid)"
  exit 0
fi

[ $# -ge 2 ] || { sed -n '2,11p' "$0" >&2; exit 2; }
TASK_ID=$1
STATUS=$2
MESSAGE_ID=${3:-$(new_uuid)}
NOW=$(date -u +%Y-%m-%dT%H:%M:%SZ)
PAYLOAD=$(printf '{"message_id": "%s", "task_id": "%s", "status": "%s", "occurred_at": "%s"}' \
  "$MESSAGE_ID" "$TASK_ID" "$STATUS" "$NOW")

echo "publishing: $PAYLOAD"
publish "task-update.$STATUS" "$PAYLOAD" "$MESSAGE_ID"
