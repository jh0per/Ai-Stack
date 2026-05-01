#!/usr/bin/env bash
set -e

cd "$HOME/ai-stack"

mkdir -p logs run

source .venv/bin/activate

echo "Starting MCP storage server..."
setsid nohup python mcp-storage-server/server.py > logs/mcp-storage-server.log 2>&1 &
echo $! > run/mcp-storage-server.pid

sleep 2

echo "Starting Ollama Agent Gateway..."
setsid nohup uvicorn ollama-agent-gateway.app:app \
  --host 127.0.0.1 \
  --port 3700 \
  > logs/ollama-agent-gateway.log 2>&1 &
echo $! > run/ollama-agent-gateway.pid

echo ""
echo "Started:"
echo "MCP PID:     $(cat run/mcp-storage-server.pid)"
echo "Gateway PID: $(cat run/ollama-agent-gateway.pid)"
echo ""
echo "Test:"
echo "curl http://127.0.0.1:3700/health"
