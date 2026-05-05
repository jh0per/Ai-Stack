#!/usr/bin/env bash
set -e

cd "$(dirname "$0")"

mkdir -p logs run

source .venv/bin/activate

if [ -f .env ]; then
  set -a
  # shellcheck disable=SC1091
  source .env
  set +a
fi

GATEWAY_HOST="${GATEWAY_HOST:-127.0.0.1}"
GATEWAY_PORT="${GATEWAY_PORT:-3700}"

echo "Starting MCP storage server..."
setsid nohup python mcp-storage-server/server.py > logs/mcp-storage-server.log 2>&1 &
echo $! > run/mcp-storage-server.pid

sleep 2

echo "Starting Ollama Agent Gateway..."
setsid nohup uvicorn ollama-agent-gateway.app:app \
  --host "$GATEWAY_HOST" \
  --port "$GATEWAY_PORT" \
  > logs/ollama-agent-gateway.log 2>&1 &
echo $! > run/ollama-agent-gateway.pid

echo ""
echo "Started:"
echo "MCP PID:     $(cat run/mcp-storage-server.pid)"
echo "Gateway PID: $(cat run/ollama-agent-gateway.pid)"
echo ""
echo "Test:"
echo "curl http://$GATEWAY_HOST:$GATEWAY_PORT/health"
echo "Browser chat: http://$GATEWAY_HOST:$GATEWAY_PORT/"
