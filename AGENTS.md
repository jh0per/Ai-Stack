# Agent Instructions

This repository contains a local storage monitoring and AI assistant stack.

## Project Layout

- `mcp-storage-server/server.py` - FastMCP tools for local storage, filesystems, SMART, Docker, and systemd.
- `ollama-agent-gateway/app.py` - FastAPI gateway that calls MCP tools, Ollama, and optional SearXNG search.
- `telegram-bot/bot.py` - Telegram bot with scheduled alerts and daily summaries.
- `searxng/` - optional local SearXNG configuration.
- `systemd/` - example service files only.

## Safety Rules

Do not commit secrets or local runtime data.

Ignored local files include:

- `.env`
- `telegram-bot/.env`
- `.state.json`
- `logs/`
- `run/`
- `.venv/`
- `__pycache__/`
- `searxng/searxng/settings.yml`

Use `.env.example`, `.evn.example`, and `telegram-bot/.env.example` for public configuration examples.

If you ever see a real Telegram token, API key, local password, private IP inventory, or machine-specific path in a publishable file, remove it and tell the user. Do not repeat the secret in your final answer.

## Storage Support

The gateway must not assume ZFS only.

Supported sources:

- generic filesystems through `df`, `findmnt`, and `lsblk`
- ZFS pools and datasets when `zpool`/`zfs` exist
- btrfs, ext4, xfs, and other mounted filesystems through generic mount usage
- SMART through `smartctl` when permissions allow it
- Sia/Storj nodes through Docker and systemd discovery

Keep ZFS-specific functionality optional. If ZFS tools are missing, generic storage status should still work.

## Local Setup

Main stack:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
./run.sh
```

Telegram bot:

```bash
cd telegram-bot
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
```

## Verification

Run syntax checks after Python edits:

```bash
.venv/bin/python -m py_compile mcp-storage-server/server.py ollama-agent-gateway/app.py telegram-bot/bot.py
```

Check endpoints after MCP/gateway changes:

```bash
curl http://127.0.0.1:3700/health
curl http://127.0.0.1:3700/mcp/tools
curl http://127.0.0.1:3700/storage/status
```

Before publishing, dry-run staged files:

```bash
git add -n .
```

Confirm no ignored runtime files or secrets are included.

## Coding Guidelines

- Prefer small, focused changes.
- Keep existing API shape stable unless the user asks for a breaking change.
- Add env flags instead of hardcoding local paths or language choices.
- Keep `LANGUAGE=uk|en` behavior in gateway responses.
- Avoid adding new dependencies unless they remove meaningful complexity.
- Do not rewrite generated SearXNG settings into a public file.
- Use structured JSON from system tools where available.
- Avoid shell scripts for logic that belongs in MCP tools.

## Runtime Notes

`SMARTCTL_USE_SUDO=1` uses `sudo -n smartctl`. It only works with passwordless sudo for smartctl. If not available, SMART details may be `unknown`; do not treat that as proof disks are healthy.

The Telegram bot still contains legacy ZFS-oriented commands. The HTTP gateway storage status is the generic filesystem-aware path.
