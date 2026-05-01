# AI Stack Storage Agent

Local FastAPI + FastMCP tooling for storage-node monitoring and Ollama-assisted answers.

It can inspect:

- Sia and Storj Docker/systemd nodes
- disk space through `df`, `findmnt`, and `lsblk`
- ZFS pools/datasets when ZFS is installed
- ext4, btrfs, xfs and other mounted filesystems through generic mount usage
- SMART identity and selected health attributes when `smartctl` is available

## Safety Before Publishing

Real secrets and runtime files are ignored by `.gitignore`:

- `.env`
- `telegram-bot/.env`
- `.state.json`
- `logs/`
- `run/`
- `.venv/`
- `searxng/searxng/settings.yml`

Use `.env.example` and `telegram-bot/.env.example` as templates.

If a Telegram bot token was ever committed, pasted into logs, or shared, revoke it in BotFather and create a new one.

## Requirements

Recommended OS: Linux with Python 3.12+.

Install system packages:

```bash
sudo apt update
sudo apt install -y python3 python3-venv python3-pip curl util-linux smartmontools
```

Optional packages:

```bash
sudo apt install -y docker.io
sudo apt install -y zfsutils-linux
```

Notes:

- `docker.io` is only needed for Docker container discovery.
- `zfsutils-linux` is only needed on ZFS hosts.
- Non-ZFS hosts still work through `df`, `findmnt`, and `lsblk`.
- SMART details may require root or passwordless sudo for `smartctl`.

## Install

Clone the repository:

```bash
git clone https://github.com/jh0per/Ai-Stack.git
cd Ai-Stack
```

Create the main virtual environment:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
```

Edit `.env`:

```bash
nano .env
```

Important options:

- `LANGUAGE=uk` or `LANGUAGE=en`
- `OLLAMA_URL=http://127.0.0.1:11434/api/chat`
- `OLLAMA_MODEL=ua-tech`
- `SMARTCTL_USE_SUDO=0` by default; set to `1` only if passwordless sudo is configured for `smartctl`
- `FS_ALLOWED_ROOTS=/mnt` or another safe comma-separated path list
- `WEB_SEARCH_ENABLED=0` unless you run local SearXNG

Start the MCP storage server and gateway:

```bash
./run.sh
```

Check status:

```bash
curl http://127.0.0.1:3700/health
curl http://127.0.0.1:3700/storage/status
```

Stop services:

```bash
./stop.sh
```

## Optional SMART Sudo

If you want SMART health without running the whole app as root, allow only `smartctl`:

```bash
sudo visudo -f /etc/sudoers.d/storage-ai-smartctl
```

Add this line, replacing `your_user`:

```text
your_user ALL=(root) NOPASSWD: /usr/sbin/smartctl
```

Then set:

```bash
SMARTCTL_USE_SUDO=1
```

in `.env`, and restart:

```bash
./stop.sh
./run.sh
```

## Telegram Bot

```bash
cd telegram-bot
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
```

Edit `telegram-bot/.env` and set `BOT_TOKEN` and `ALLOWED_CHAT_ID`.
Set `STORAGE_STATUS_URL` if the gateway is not on `http://127.0.0.1:3700/storage/status`.

Run manually:

```bash
python bot.py
```

Install as a systemd service:

```bash
sudo cp ../systemd/zfs-ai-telegram.service.example /etc/systemd/system/zfs-ai-telegram.service
sudo nano /etc/systemd/system/zfs-ai-telegram.service
```

Adjust:

- `User=...`
- `WorkingDirectory=.../Ai-Stack/telegram-bot`
- `EnvironmentFile=.../Ai-Stack/telegram-bot/.env`
- `ExecStart=.../Ai-Stack/telegram-bot/.venv/bin/python .../Ai-Stack/telegram-bot/bot.py`

Enable and start:

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now zfs-ai-telegram.service
systemctl status zfs-ai-telegram.service --no-pager
```

The current Telegram bot commands are still ZFS-oriented. The HTTP gateway status endpoint is filesystem-generic and supports non-ZFS mounts.

Useful Telegram commands:

- `/status` - main generic storage/Sia/Storj/filesystem status from the gateway
- `/problems` - only storage problems from the gateway
- `/smart` - SMART and disk health summary from the gateway
- `/storage` - alias-style explicit storage status command
- `/health` - alias for `/status`
- `/disks` - alias for `/smart`
- `/zfs` - legacy ZFS-focused status
- `/pools`, `/datasets`, `/raw`, `/intro`, `/askzfs` - ZFS-only commands

## Optional SearXNG

Local web search is optional.

```bash
cd searxng
cp settings.yml.example searxng/settings.yml
docker compose up -d
```

Then set in `.env`:

```bash
WEB_SEARCH_ENABLED=1
SEARXNG_URL=http://127.0.0.1:8888/search
SMART_WEB_DEFAULT=1
```

## Public Repository Checklist

Before pushing your own fork, check what would be committed:

```bash
git add -n .
```

These must not appear:

- `.env`
- `telegram-bot/.env`
- `.state.json`
- `.venv/`
- `logs/`
- `run/`
- `searxng/searxng/settings.yml`

Run a quick secret scan:

```bash
rg -n "BOT_TOKEN=|ALLOWED_CHAT_ID=[0-9]+|secret_key:|PASSWORD|TOKEN|SECRET|API_KEY" --hidden --glob '!.git/**' --glob '!*.env' .
```

## Troubleshooting

Check gateway logs:

```bash
tail -n 100 logs/ollama-agent-gateway.log
tail -n 100 logs/mcp-storage-server.log
```

Check Telegram service logs:

```bash
journalctl -u zfs-ai-telegram.service -n 100 --no-pager
```

If `/storage/status` reports `unknown` SMART drives, verify:

```bash
smartctl --scan-open
sudo -n smartctl --scan-open
```
