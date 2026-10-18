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
- `OLLAMA_MODEL=qwen3.5:9b`
- `GATEWAY_HOST=127.0.0.1` for local-only browser access; use `0.0.0.0` only on a trusted network
- `GATEWAY_PORT=3700`
- `SMARTCTL_USE_SUDO=0` by default; set to `1` only if passwordless sudo is configured for `smartctl`
- `SMART_LOG_DIR=/var/lib/smartmontools` for passive SMART data from smartd attribute logs
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

Local terminal monitoring commands:

```bash
./scripts/smart-table
./scripts/zfs-table
./scripts/node-table /path/to/node-configs
```

`node-table` builds separate Storj and Sia inventories from live Docker
configuration. The required path limits discovery to containers whose run,
Compose, data, or storage paths are below that directory. All displayed values
are collected at runtime; the script contains no node inventory, credentials,
addresses, or machine-specific paths.

The default view groups fields so the tables fit the detected terminal width:

```bash
./scripts/node-table /path/to/node-configs --only storj
./scripts/node-table /path/to/node-configs --only sia
./scripts/node-table /path/to/node-configs --width 120
./scripts/node-table /path/to/node-configs --wide
./scripts/node-table /path/to/node-configs --json
```

Storj addresses come from the container's `ADDRESS` setting. Sia listen
addresses come from the running container command. Hosting providers and other
external annotations are intentionally not guessed. The script is safe to
publish, but its runtime output can contain real addresses and filesystem paths,
so treat captured output as private operational data.

To make a command available system-wide, symlink it from the checked-out repository, for example:

```bash
sudo ln -s "$(pwd)/scripts/zfs-table" /usr/local/sbin/zfs-table
sudo ln -s "$(pwd)/scripts/node-table" /usr/local/sbin/node-table
```

Open the browser chat:

```text
http://127.0.0.1:3700/
```

Set `GATEWAY_HOST` in your local `.env` to a trusted LAN address if you need access from another device. Do not commit machine-specific addresses.

The chat page can use the routed agent, plain Ollama, web-assisted answers, or ZFS/storage mode.

## Independent Storage Watchdog

`scripts/storage-watchdog` is a root-level, config-driven watchdog that can run
independently of the gateway and Telegram bot process. It can monitor expected
ZFS pools, Docker containers, HTTP endpoints, PCI controller drivers, active
SMART long tests, and recent kernel storage errors. It sends alerts on state
changes, repeats unresolved alerts at a configurable interval, and sends a
recovery notification.

The repository intentionally contains no real host inventory. Copy the example
configuration outside the repository and fill in local values there:

```bash
sudo install -m 0755 scripts/storage-watchdog /usr/local/sbin/storage-watchdog
sudo install -m 0600 systemd/storage-watchdog.example.json /etc/storage-watchdog.json
sudo nano /etc/storage-watchdog.json
```

Create a root-only Telegram environment file referenced by the JSON config:

```bash
sudo install -m 0600 /dev/null /etc/storage-watchdog.telegram.env
sudo nano /etc/storage-watchdog.telegram.env
```

It must define `BOT_TOKEN` and `ALLOWED_CHAT_ID`. Never add the populated JSON
or Telegram environment file to Git. Pool names, node ports, disk identifiers,
local addresses, and captured watchdog output are private deployment data.

Validate the configuration without sending a message:

```bash
sudo /usr/local/sbin/storage-watchdog --config /etc/storage-watchdog.json --print-only
```

Install and enable the timer:

```bash
sudo install -m 0644 systemd/storage-watchdog.service /etc/systemd/system/
sudo install -m 0644 systemd/storage-watchdog.timer /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now storage-watchdog.timer
```

Optionally verify Telegram delivery:

```bash
sudo /usr/local/sbin/storage-watchdog --config /etc/storage-watchdog.json --test-message
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

Scheduled Telegram alerts use the generic gateway status and only send critical issues by default. Capacity/fullness warnings are available on demand with `/space` so routine high Sia/Storj allocation does not spam alerts. If the critical alert signature is unchanged, repeats are suppressed for `ALERT_UNCHANGED_COOLDOWN_HOURS` hours.

The `/status` command reports only detected problems; `/storage` presents a readable overview of issues, nodes, pools, busiest filesystems, node allocations, and SMART health. The daily summary additionally includes all migrated `zfs-table` filesystem/volume rows and passive `smart-table` disk snapshots. `/smart` shows those snapshots plus live NVMe health, while `/space` includes the full ZFS dataset table. When smartd logs are available, the gateway does not query spinning disks; it only reads the logs and checks non-spinning NVMe devices. Full live `smartctl` scanning remains a fallback for hosts without logs.

NAS backup mount monitoring is enabled by default. Set `NAS_MOUNT_PATHS=/mnt/nas-backup` (or your local mountpoint) and `NAS_EXPECTED_FSTYPES=cifs,nfs,nfs4,fuse.sshfs` in `telegram-bot/.env`. The bot sends an alert when the NAS mount disappears and a recovery message when it returns.

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

- `/status` - only detected storage problems from the gateway
- `/problems` - explicit alias for `/status`
- `/space` - pool, filesystem, and node allocation fullness from the gateway
- `/smart` - SMART and disk health summary from the gateway
- `/nas` - NAS mount status used by backup protection
- `/storage` - readable storage/Sia/Storj/filesystem overview from the gateway
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
