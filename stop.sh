#!/usr/bin/env bash
set -e

cd "$(dirname "$0")"

if [ -f run/ollama-agent-gateway.pid ]; then
  PID=$(cat run/ollama-agent-gateway.pid)
  echo "Stopping Gateway PID $PID"
  kill "$PID" 2>/dev/null || true
  rm -f run/ollama-agent-gateway.pid
fi

if [ -f run/mcp-storage-server.pid ]; then
  PID=$(cat run/mcp-storage-server.pid)
  echo "Stopping MCP PID $PID"
  kill "$PID" 2>/dev/null || true
  rm -f run/mcp-storage-server.pid
fi

echo "Stopped."
