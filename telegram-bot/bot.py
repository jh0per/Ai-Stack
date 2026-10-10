import asyncio
import hashlib
import json
import os
import socket
import subprocess
from dataclasses import dataclass
from datetime import datetime, time as dt_time, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import requests
from dotenv import load_dotenv
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes


BASE_DIR = Path(__file__).resolve().parent
ENV_FILE = BASE_DIR / ".env"
STATE_FILE = BASE_DIR / ".state.json"

load_dotenv(ENV_FILE)


BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
ALLOWED_CHAT_ID = os.getenv("ALLOWED_CHAT_ID", "").strip()

TIMEZONE = os.getenv("TIMEZONE", "UTC").strip()

ALERT_ENABLED = os.getenv("ALERT_ENABLED", "1").strip() == "1"
ALERT_INTERVAL_MINUTES = int(os.getenv("ALERT_INTERVAL_MINUTES", "60"))
ALERT_ONLY_ON_CHANGE = os.getenv("ALERT_ONLY_ON_CHANGE", "1").strip() == "1"
ALERT_UNCHANGED_COOLDOWN_HOURS = float(os.getenv("ALERT_UNCHANGED_COOLDOWN_HOURS", "6"))

DAILY_SUMMARY_ENABLED = os.getenv("DAILY_SUMMARY_ENABLED", "1").strip() == "1"
DAILY_TIME = os.getenv("DAILY_TIME", "09:00").strip()

WARN_POOL_CAP = float(os.getenv("WARN_POOL_CAP", "80"))
CRIT_POOL_CAP = float(os.getenv("CRIT_POOL_CAP", "90"))
MIN_POOL_FREE_GB = float(os.getenv("MIN_POOL_FREE_GB", "500"))
WARN_DATASET_CAP = float(os.getenv("WARN_DATASET_CAP", "90"))

AI_ENABLED = os.getenv("AI_ENABLED", "0").strip() == "1"
OLLAMA_URL = os.getenv("OLLAMA_URL", "http://127.0.0.1:11434/api/chat").strip()
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "qwen3.5:9b").strip()

ZPOOL = os.getenv("ZPOOL_BIN", "/usr/sbin/zpool").strip()
ZFS = os.getenv("ZFS_BIN", "/usr/sbin/zfs").strip()
STORAGE_STATUS_URL = os.getenv(
    "STORAGE_STATUS_URL",
    "http://127.0.0.1:3700/storage/status",
).strip()
FINDMNT = os.getenv("FINDMNT_BIN", "/usr/bin/findmnt").strip()
MOUNTPOINT = os.getenv("MOUNTPOINT_BIN", "/usr/bin/mountpoint").strip()

NAS_MONITOR_ENABLED = os.getenv("NAS_MONITOR_ENABLED", "1").strip() == "1"
NAS_MOUNT_PATHS = [
    item.strip()
    for item in os.getenv("NAS_MOUNT_PATHS", "/mnt/nas-backup").split(",")
    if item.strip()
]
NAS_EXPECTED_FSTYPES = {
    item.strip()
    for item in os.getenv("NAS_EXPECTED_FSTYPES", "cifs,nfs,nfs4,fuse.sshfs").split(",")
    if item.strip()
}
NAS_ALERT_UNCHANGED_COOLDOWN_HOURS = float(
    os.getenv("NAS_ALERT_UNCHANGED_COOLDOWN_HOURS", str(ALERT_UNCHANGED_COOLDOWN_HOURS))
)


@dataclass
class Problem:
    level: str
    title: str
    details: str


def run_cmd(cmd: list[str], timeout: int = 60) -> tuple[str, str, int]:
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=timeout,
            check=False,
        )
        return result.stdout.strip(), result.stderr.strip(), result.returncode
    except Exception as e:
        return "", str(e), 999


def parse_int(value: str):
    value = value.strip()
    if value in ("", "-", "none", "None"):
        return None

    try:
        return int(value)
    except ValueError:
        return None


def parse_percent(value: str):
    value = value.strip().replace("%", "")
    if value in ("", "-", "none", "None"):
        return None

    try:
        return float(value)
    except ValueError:
        return None


def fmt_bytes(num):
    if num is None:
        return "-"

    num = float(num)
    units = ["B", "K", "M", "G", "T", "P"]
    i = 0

    while num >= 1024 and i < len(units) - 1:
        num /= 1024
        i += 1

    if i <= 2:
        return f"{num:.0f}{units[i]}"

    return f"{num:.2f}{units[i]}"


def split_message(text: str, limit: int = 3900) -> list[str]:
    if len(text) <= limit:
        return [text]

    chunks = []

    while len(text) > limit:
        cut = text.rfind("\n", 0, limit)
        if cut == -1:
            cut = limit

        chunks.append(text[:cut])
        text = text[cut:].lstrip()

    if text:
        chunks.append(text)

    return chunks


def now_text() -> str:
    return datetime.now(ZoneInfo(TIMEZONE)).strftime("%Y-%m-%d %H:%M:%S %Z")


def load_state() -> dict:
    if not STATE_FILE.exists():
        return {}

    try:
        return json.loads(STATE_FILE.read_text())
    except Exception:
        return {}


def save_state(state: dict):
    STATE_FILE.write_text(json.dumps(state, indent=2, ensure_ascii=False))


def get_pools() -> tuple[list[dict], str]:
    stdout, stderr, code = run_cmd([
        ZPOOL,
        "list",
        "-Hp",
        "-o",
        "name,size,alloc,free,cap,health,frag",
    ])

    if code != 0:
        return [], f"zpool list error: {stderr or stdout}"

    pools = []

    for line in stdout.splitlines():
        parts = line.split("\t")

        if len(parts) < 7:
            continue

        name, size, alloc, free, cap, health, frag = parts[:7]

        pools.append({
            "name": name,
            "size": parse_int(size),
            "alloc": parse_int(alloc),
            "free": parse_int(free),
            "cap": parse_percent(cap),
            "health": health.strip(),
            "frag": parse_percent(frag),
        })

    return pools, ""


def get_datasets() -> tuple[list[dict], str]:
    stdout, stderr, code = run_cmd([
        ZFS,
        "list",
        "-Hp",
        "-o",
        "name,used,avail,refer,quota,refquota,mountpoint",
        "-t",
        "filesystem,volume",
    ])

    if code != 0:
        return [], f"zfs list error: {stderr or stdout}"

    datasets = []

    for line in stdout.splitlines():
        parts = line.split("\t")

        if len(parts) < 7:
            continue

        name, used, avail, refer, quota, refquota, mountpoint = parts[:7]

        used_b = parse_int(used)
        avail_b = parse_int(avail)
        quota_b = parse_int(quota)
        refquota_b = parse_int(refquota)

        limit_b = refquota_b or quota_b
        usage_pct = None

        if limit_b and limit_b > 0 and used_b is not None:
            usage_pct = used_b / limit_b * 100

        datasets.append({
            "name": name,
            "used": used_b,
            "avail": parse_int(avail),
            "refer": parse_int(refer),
            "quota": quota_b,
            "refquota": refquota_b,
            "limit": limit_b,
            "usage_pct": usage_pct,
            "mountpoint": mountpoint,
        })

    return datasets, ""


def analyze_zfs() -> dict:
    hostname = socket.gethostname()
    pools, pool_error = get_pools()
    datasets, dataset_error = get_datasets()

    problems: list[Problem] = []

    if pool_error:
        problems.append(Problem("🔴", "zpool list не спрацював", pool_error))

    if dataset_error:
        problems.append(Problem("🔴", "zfs list не спрацював", dataset_error))

    status_x, status_x_err, status_x_code = run_cmd([ZPOOL, "status", "-x"])

    if status_x_code != 0:
        problems.append(
            Problem(
                "🔴",
                "zpool status -x не спрацював",
                status_x_err or status_x,
            )
        )
    else:
        normalized_status = status_x.lower()
        if status_x and "all pools are healthy" not in normalized_status:
            problems.append(
                Problem(
                    "🔴",
                    "zpool status -x показує проблему",
                    status_x,
                )
            )

    for pool in pools:
        cap = pool["cap"]
        free_gb = (pool["free"] or 0) / 1024 / 1024 / 1024

        if pool["health"] != "ONLINE":
            problems.append(
                Problem(
                    "🔴",
                    f"Pool {pool['name']} не ONLINE",
                    f"health={pool['health']}",
                )
            )

        if cap is not None and cap >= CRIT_POOL_CAP:
            problems.append(
                Problem(
                    "🔴",
                    f"Pool {pool['name']} критично заповнений",
                    f"cap={cap:.1f}%, free={fmt_bytes(pool['free'])}",
                )
            )
        elif cap is not None and cap >= WARN_POOL_CAP:
            problems.append(
                Problem(
                    "🟡",
                    f"Pool {pool['name']} сильно заповнений",
                    f"cap={cap:.1f}%, free={fmt_bytes(pool['free'])}",
                )
            )

        if free_gb < MIN_POOL_FREE_GB:
            problems.append(
                Problem(
                    "🟡",
                    f"Pool {pool['name']} має мало вільного місця",
                    f"free={free_gb:.0f}G, threshold={MIN_POOL_FREE_GB:.0f}G",
                )
            )

    quota_warnings = []

    for ds in datasets:
        pct = ds["usage_pct"]

        if pct is not None and pct >= WARN_DATASET_CAP:
            quota_warnings.append(ds)

    quota_warnings.sort(key=lambda x: x["usage_pct"] or 0, reverse=True)

    for ds in quota_warnings[:20]:
        problems.append(
            Problem(
                "🟡",
                f"Dataset близько до quota: {ds['name']}",
                f"{fmt_bytes(ds['used'])}/{fmt_bytes(ds['limit'])} ({ds['usage_pct']:.1f}%)",
            )
        )

    has_red = any(p.level == "🔴" for p in problems)
    has_yellow = any(p.level == "🟡" for p in problems)

    if has_red:
        overall = "🔴 Є критичні проблеми"
    elif has_yellow:
        overall = "🟡 Є попередження"
    else:
        overall = "🟢 Все ок"

    return {
        "hostname": hostname,
        "time": now_text(),
        "overall": overall,
        "pools": pools,
        "datasets": datasets,
        "problems": problems,
        "status_x": status_x,
    }


def problem_signature_from_data(data: dict) -> str:
    problems: list[Problem] = data["problems"]

    stable = [
        {
            "level": p.level,
            "title": p.title,
            "details": p.details,
        }
        for p in problems
    ]

    payload = json.dumps(stable, ensure_ascii=False, sort_keys=True)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def storage_problem_signature(data: dict) -> str:
    stable = {
        "status": data.get("status"),
        "issues": [
            {
                "severity": issue.get("severity"),
                "status": issue.get("status"),
                "area": issue.get("area"),
                "message": issue.get("message"),
                "action": issue.get("action"),
            }
            for issue in data.get("issues", [])
        ],
    }

    payload = json.dumps(stable, ensure_ascii=False, sort_keys=True)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def critical_storage_issues(data: dict) -> list[dict]:
    return [
        issue for issue in data.get("issues", [])
        if int(issue.get("severity") or 0) >= 3
    ]


def critical_storage_signature(data: dict) -> str:
    stable = {
        "critical_issues": [
            {
                "severity": issue.get("severity"),
                "status": issue.get("status"),
                "area": issue.get("area"),
                "message": issue.get("message"),
                "action": issue.get("action"),
            }
            for issue in critical_storage_issues(data)
        ],
    }

    payload = json.dumps(stable, ensure_ascii=False, sort_keys=True)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def parse_state_time(value: str):
    if not value:
        return None

    try:
        timestamp = value.rsplit(" ", 1)[0]
        return datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S").replace(tzinfo=ZoneInfo(TIMEZONE))
    except Exception:
        return None


def unchanged_alert_in_cooldown(
    state: dict,
    signature: str,
    signature_key: str = "last_storage_problem_signature",
    time_key: str = "last_storage_problem_time",
    cooldown_hours: float | None = None,
) -> bool:
    if not ALERT_ONLY_ON_CHANGE:
        return False

    if state.get(signature_key) != signature:
        return False

    last_sent = parse_state_time(state.get(time_key, ""))
    if not last_sent:
        return False

    if cooldown_hours is None:
        cooldown_hours = ALERT_UNCHANGED_COOLDOWN_HOURS

    elapsed = datetime.now(ZoneInfo(TIMEZONE)) - last_sent
    return elapsed < timedelta(hours=cooldown_hours)


def inspect_nas_mount(path: str) -> dict:
    status = {
        "path": path,
        "ok": False,
        "state": "unknown",
        "source": "-",
        "fstype": "-",
        "message": "",
        "action": "",
    }

    target = Path(path)

    if not target.exists():
        status.update({
            "state": "missing",
            "message": f"NAS mount path does not exist: {path}",
            "action": "Create the mountpoint directory and mount the NAS share before backups run.",
        })
        return status

    _, stderr, code = run_cmd([MOUNTPOINT, "-q", path], timeout=10)
    if code != 0:
        detail = stderr.strip()
        if detail:
            detail = f" ({detail})"

        status.update({
            "state": "unmounted",
            "message": f"NAS is not mounted at {path}{detail}",
            "action": "Mount the NAS share. Backups must not write to the local mountpoint directory.",
        })
        return status

    stdout, stderr, code = run_cmd(
        [FINDMNT, "-n", "-T", path, "-o", "SOURCE,FSTYPE"],
        timeout=10,
    )
    if code != 0 or not stdout.strip():
        status.update({
            "state": "unknown",
            "message": f"NAS mount {path} is mounted, but findmnt did not return source/fstype.",
            "action": stderr or "Check findmnt and mount table.",
        })
        return status

    parts = stdout.split(None, 1)
    source = parts[0] if parts else "-"
    fstype = parts[1].strip() if len(parts) > 1 else "-"

    status["source"] = source
    status["fstype"] = fstype

    if NAS_EXPECTED_FSTYPES and fstype not in NAS_EXPECTED_FSTYPES:
        status.update({
            "state": "wrong_fstype",
            "message": f"NAS mount {path} has unexpected filesystem type {fstype}.",
            "action": f"Expected one of: {', '.join(sorted(NAS_EXPECTED_FSTYPES))}. Check that this is the real NAS mount.",
        })
        return status

    status.update({
        "ok": True,
        "state": "mounted",
        "message": f"NAS mounted at {path}",
        "action": "",
    })
    return status


def get_nas_statuses() -> list[dict]:
    if not NAS_MONITOR_ENABLED:
        return []

    return [inspect_nas_mount(path) for path in NAS_MOUNT_PATHS]


def nas_alert_issues(statuses: list[dict] | None = None) -> list[dict]:
    if statuses is None:
        statuses = get_nas_statuses()

    return [item for item in statuses if not item.get("ok")]


def nas_alert_signature(issues: list[dict]) -> str:
    stable = [
        {
            "path": issue.get("path"),
            "state": issue.get("state"),
            "source": issue.get("source"),
            "fstype": issue.get("fstype"),
            "message": issue.get("message"),
            "action": issue.get("action"),
        }
        for issue in issues
    ]

    payload = json.dumps(stable, ensure_ascii=False, sort_keys=True)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def build_nas_report(statuses: list[dict] | None = None) -> str:
    if not NAS_MONITOR_ENABLED:
        return "NAS monitor disabled. Set NAS_MONITOR_ENABLED=1 to enable it."

    if statuses is None:
        statuses = get_nas_statuses()

    lines = [
        "🗄 NAS mount status",
        f"🖥 Host: {socket.gethostname()}",
        f"🕒 {now_text()}",
        "",
    ]

    if not NAS_MOUNT_PATHS:
        lines.append("NAS_MOUNT_PATHS is empty; nothing is monitored.")
        return "\n".join(lines).strip()

    for item in statuses:
        icon = "✅" if item.get("ok") else "🚨"
        lines.append(
            f"{icon} {item.get('path')}: {item.get('state')} | "
            f"fs={item.get('fstype')} | source={item.get('source')}"
        )
        if item.get("message"):
            lines.append(f"   {item['message']}")
        if item.get("action"):
            lines.append(f"   Дія: {item['action']}")

    return "\n".join(lines).strip()


def build_status_report(data: dict | None = None) -> str:
    if data is None:
        data = analyze_zfs()

    problems: list[Problem] = data["problems"]

    lines = [
        f"{data['overall']}",
        f"🖥 Host: {data['hostname']}",
        f"🕒 {data['time']}",
        "",
    ]

    if problems:
        lines.append("Проблеми:")

        for p in problems[:20]:
            lines.append(f"{p.level} {p.title}")

            if p.details:
                lines.append(f"   {p.details}")

        if len(problems) > 20:
            lines.append(f"...ще {len(problems) - 20} проблем")
    else:
        lines.append("✅ Усі пули ONLINE, критичних проблем не знайдено.")

    lines.append("")
    lines.append("Pools:")

    for pool in data["pools"]:
        cap = "-"
        frag = "-"

        if pool["cap"] is not None:
            cap = f"{pool['cap']:.1f}%"

        if pool["frag"] is not None:
            frag = f"{pool['frag']:.0f}%"

        lines.append(
            f"• {pool['name']}: {pool['health']} | "
            f"used {fmt_bytes(pool['alloc'])}/{fmt_bytes(pool['size'])} | "
            f"free {fmt_bytes(pool['free'])} | "
            f"cap {cap} | frag {frag}"
        )

    return "\n".join(lines)


def build_problems_report(data: dict | None = None) -> str:
    if data is None:
        data = analyze_zfs()

    problems: list[Problem] = data["problems"]

    lines = [
        f"{data['overall']}",
        f"🖥 Host: {data['hostname']}",
        f"🕒 {data['time']}",
        "",
    ]

    if not problems:
        lines.append("✅ Проблем не знайдено.")
        return "\n".join(lines)

    lines.append("Знайдено:")

    for p in problems:
        lines.append(f"{p.level} {p.title}")

        if p.details:
            lines.append(f"   {p.details}")

    return "\n".join(lines)


def build_pools_report() -> str:
    pools, error = get_pools()

    if error:
        return f"🔴 {error}"

    lines = [
        "📦 ZFS pools:",
        f"🕒 {now_text()}",
        "",
    ]

    for pool in pools:
        cap = "-"
        frag = "-"

        if pool["cap"] is not None:
            cap = f"{pool['cap']:.1f}%"

        if pool["frag"] is not None:
            frag = f"{pool['frag']:.0f}%"

        lines.append(
            f"• {pool['name']}: {pool['health']} | "
            f"size {fmt_bytes(pool['size'])} | "
            f"alloc {fmt_bytes(pool['alloc'])} | "
            f"free {fmt_bytes(pool['free'])} | "
            f"cap {cap} | frag {frag}"
        )

    return "\n".join(lines)


def build_datasets_report() -> str:
    datasets, error = get_datasets()

    if error:
        return f"🔴 {error}"

    top = sorted(
        datasets,
        key=lambda x: x["used"] or 0,
        reverse=True,
    )[:30]

    lines = [
        "📊 Top ZFS datasets by used space:",
        f"🕒 {now_text()}",
        "",
    ]

    for ds in top:
        quota = fmt_bytes(ds["limit"])

        usage = "-"
        if ds["usage_pct"] is not None:
            usage = f"{ds['usage_pct']:.1f}%"

        lines.append(
            f"• {ds['name']}: "
            f"used {fmt_bytes(ds['used'])}, "
            f"avail {fmt_bytes(ds['avail'])}, "
            f"quota {quota}, "
            f"usage {usage}"
        )

    return "\n".join(lines)


def build_raw_report() -> str:
    commands = [
        ("zpool list", [ZPOOL, "list"]),
        ("zpool status -x", [ZPOOL, "status", "-x"]),
        ("zpool status", [ZPOOL, "status"]),
        (
            "zfs list",
            [
                ZFS,
                "list",
                "-o",
                "name,used,avail,refer,quota,refquota,mountpoint",
                "-t",
                "filesystem,volume",
            ],
        ),
    ]

    parts = [
        f"🖥 Host: {socket.gethostname()}",
        f"🕒 {now_text()}",
        "",
    ]

    for title, cmd in commands:
        stdout, stderr, code = run_cmd(cmd)

        parts.append("=" * 60)
        parts.append(title)
        parts.append("=" * 60)

        if stdout:
            parts.append(stdout)

        if stderr:
            parts.append("STDERR:")
            parts.append(stderr)

        if code != 0:
            parts.append(f"EXIT_CODE={code}")

        parts.append("")

    return "\n".join(parts)


def build_ai_context() -> str:
    data = analyze_zfs()
    status = build_status_report(data)
    datasets = build_datasets_report()

    return f"{status}\n\n{datasets}"


def ask_ollama(question: str, with_zfs_context: bool = False) -> str:
    if not AI_ENABLED:
        return "AI вимкнений. Постав AI_ENABLED=1 в .env."

    question = question.strip()

    if not question:
        return "Напиши питання після команди. Наприклад:\n/ask що таке zpool scrub?"

    system_prompt = """
Ти корисний технічний асистент.
Відповідай українською, коротко і по суті.
Якщо питання про ZFS/Linux/storage — відповідай як досвідчений адмін.
Не вигадуй фактів, якщо даних недостатньо.
Не радь небезпечні дії без попередження.
""".strip()

    if with_zfs_context:
        zfs_context = build_ai_context()

        user_prompt = f"""
Ось поточний ZFS статус сервера:

{zfs_context}

Питання користувача:

{question}
""".strip()
    else:
        user_prompt = question

    try:
        response = requests.post(
            OLLAMA_URL,
            json={
                "model": OLLAMA_MODEL,
                "stream": False,
                "messages": [
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt},
                ],
                "options": {
                    "temperature": 0.2,
                    "num_ctx": 8192,
                },
            },
            timeout=180,
        )

        response.raise_for_status()
        data = response.json()

        return data["message"]["content"].strip()

    except Exception as e:
        return f"⚠️ Ollama не відповіла: {e}"


def ask_ai_for_intro() -> str:
    if not AI_ENABLED:
        return "AI вимкнений. Постав AI_ENABLED=1 в .env."

    context = build_ai_context()

    system_prompt = """
Ти storage/ZFS адміністратор.
Проаналізуй статус ZFS і дай дуже коротке інтро українською.

Правила:
- Не вигадуй того, чого нема в даних.
- Якщо все ок — так і скажи.
- Якщо є проблема — першим рядком скажи, де саме.
- Максимум 6 коротких рядків.
- Не радь видаляти дані.
- Формат має бути як короткий Telegram summary.
""".strip()

    user_prompt = f"""
Ось дані:

{context}

Дай коротке Telegram-intro у форматі:

🟢/🟡/🔴 Стан: ...
Головне: ...
Проблеми: ...
Дія: ...
""".strip()

    try:
        response = requests.post(
            OLLAMA_URL,
            json={
                "model": OLLAMA_MODEL,
                "stream": False,
                "messages": [
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt},
                ],
                "options": {
                    "temperature": 0.1,
                    "num_ctx": 8192,
                },
            },
            timeout=180,
        )

        response.raise_for_status()
        data = response.json()

        return data["message"]["content"].strip()

    except Exception as e:
        return f"⚠️ AI не відповів: {e}"


def build_intro_report() -> str:
    intro = ask_ai_for_intro()

    return (
        f"🤖 ZFS AI intro\n"
        f"🖥 Host: {socket.gethostname()}\n"
        f"🕒 {now_text()}\n\n"
        f"{intro}"
    )


def fetch_storage_status() -> dict:
    response = requests.get(STORAGE_STATUS_URL, timeout=60)
    response.raise_for_status()
    return response.json()


def metric(value, suffix: str = "") -> str:
    return "-" if value is None else f"{value}{suffix}"


def smart_table_lines(data: dict) -> list[str]:
    drives = data.get("smart_table") or data.get("drives", [])
    if not drives:
        return ["SMART snapshot-и недоступні."]

    connected = sum(1 for drive in drives if drive.get("connected") is not False)
    historical = len(drives) - connected
    lines = [f"SMART snapshots: {connected} connected, {historical} historical"]

    for drive in drives:
        connected_now = drive.get("connected") is not False
        icon = "✅" if connected_now and drive.get("status") == "ok" else "⚠️" if connected_now else "○"
        name = drive.get("device") or drive.get("name") or "?"
        model = drive.get("model") or "-"
        serial = drive.get("serial") or "-"
        lines.append(f"{icon} {name} · {model} · {serial} · {drive.get('protocol') or drive.get('type') or '-'}")
        lines.append(
            "   "
            f"temp={metric(drive.get('temperature_c'), '°C')} "
            f"relo={metric(drive.get('reallocated'))} "
            f"pend={metric(drive.get('pending'))} "
            f"uncor={metric(drive.get('uncorrectable'))} "
            f"hours={metric(drive.get('power_on_hours'))} "
            f"snap={drive.get('snapshot_at') or '-'}"
        )

    return lines


def zfs_table_lines(data: dict) -> list[str]:
    datasets = data.get("zfs_table") or []
    if not datasets:
        return ["ZFS Sia/Storj datasets недоступні."]

    storj_count = sum(1 for item in datasets if item.get("tag") == "storj")
    sia_count = sum(1 for item in datasets if item.get("tag") == "sia")
    root_count = sum(1 for item in datasets if item.get("is_root"))
    lines = [
        f"ZFS tree: {len(datasets)} entries, {root_count} roots, "
        f"{len(datasets) - root_count} children, Storj {storj_count}, Sia {sia_count}"
    ]

    by_name = {item.get("name"): item for item in datasets if item.get("name")}
    children = {name: [] for name in by_name}
    roots = []
    for item in datasets:
        name = item.get("name")
        if not name:
            continue
        parent = item.get("parent")
        if parent in by_name:
            children[parent].append(name)
        else:
            roots.append(name)

    ordered = []

    def visit(name: str, prefix: str, connector: str):
        label = name if not connector else f"{prefix}{connector}{name.rsplit('/', 1)[-1]}"
        ordered.append((by_name[name], label))
        names = children[name]
        child_prefix = prefix + ("    " if connector == "└── " else "│   " if connector else "")
        for index, child_name in enumerate(names):
            visit(
                child_name,
                child_prefix,
                "└── " if index == len(names) - 1 else "├── ",
            )

    for root in roots:
        visit(root, "", "")

    for item, label in ordered:
        lines.append(
            f"• {label}: "
            f"used={fmt_bytes(item.get('used_bytes'))} "
            f"free={fmt_bytes(item.get('available_bytes'))} "
            f"mount={item.get('mountpoint') or '-'}"
        )

    return lines


def report_status_icon(status: str) -> str:
    return {
        "ok": "🟢",
        "warning": "🟡",
        "critical": "🔴",
        "unknown": "⚪",
    }.get(str(status).lower(), "⚪")


def report_percent(value) -> str:
    try:
        return f"{float(value):.0f}%"
    except (TypeError, ValueError):
        return "-"


def report_number(value) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def build_storage_report_from_data(data: dict) -> str:
    issues = data.get("issues", [])
    recommendations = data.get("recommendations", [])
    nodes = data.get("nodes", [])
    drives = data.get("smart_table") or data.get("drives") or []
    pools = data.get("pools", [])
    filesystems = data.get("filesystems", [])
    node_storage = data.get("node_storage", [])
    status = str(data.get("status") or "unknown").lower()
    shown_issues = issues[:10]
    running_nodes = sum(1 for node in nodes if node.get("status") == "ok")
    online_pools = sum(
        1 for pool in pools if str(pool.get("health") or "").upper() == "ONLINE"
    )

    lines = [
        f"🗄 Storage · {report_status_icon(status)} {status.upper()}",
        f"🕒 {data.get('checked_at', '-')}",
    ]

    if issues:
        lines.extend(["", f"🚨 Проблеми: {len(issues)}"])
        for issue in shown_issues:
            icon = report_status_icon(issue.get("status") or "unknown")
            lines.append(
                f"{icon} [{str(issue.get('area') or '-').upper()}] "
                f"{issue.get('message') or '-'}"
            )
            if issue.get("action"):
                lines.append(f"   ↳ {issue['action']}")
        if len(issues) > len(shown_issues):
            lines.append(f"…ще {len(issues) - len(shown_issues)} у /status")
    else:
        lines.extend(["", "✅ Проблем не знайдено"])

    lines.extend(["", f"🧩 Ноди: {running_nodes}/{len(nodes)} працюють"])
    project_parts = []
    for project in ("storj", "sia"):
        project_nodes = [node for node in nodes if node.get("project") == project]
        if project_nodes:
            project_ok = sum(1 for node in project_nodes if node.get("status") == "ok")
            project_parts.append(f"{project.capitalize()} {project_ok}/{len(project_nodes)}")
    if project_parts:
        lines.append("   " + " · ".join(project_parts))
    for node in [node for node in nodes if node.get("status") != "ok"][:6]:
        lines.append(
            f"   {report_status_icon(node.get('status') or 'unknown')} "
            f"{node.get('name') or '?'} · {node.get('runtime_status') or node.get('state') or '-'}"
        )

    lines.extend(["", f"💧 ZFS pools: {online_pools}/{len(pools)} ONLINE"])
    ranked_pools = sorted(
        pools,
        key=lambda pool: (
            str(pool.get("health") or "").upper() == "ONLINE",
            -(report_number(pool.get("capacity_percent")) or 0),
        ),
    )
    for pool in ranked_pools[:8]:
        health = str(pool.get("health") or "unknown").upper()
        capacity = report_number(pool.get("capacity_percent"))
        if health != "ONLINE" or (capacity is not None and capacity >= 90):
            pool_icon = "🔴"
        elif capacity is not None and capacity >= 80:
            pool_icon = "🟡"
        else:
            pool_icon = "🟢"
        lines.append(
            f"{pool_icon} {pool.get('name') or '?'} · "
            f"{report_percent(pool.get('capacity_percent'))} used · "
            f"{pool.get('free') or '-'} free"
        )
    if len(ranked_pools) > 8:
        lines.append(f"…ще {len(ranked_pools) - 8} у /space")
    elif not ranked_pools:
        lines.append("⚪ Дані недоступні")

    ranked_filesystems = sorted(
        [item for item in filesystems if report_number(item.get("use_percent")) is not None],
        key=lambda item: report_number(item.get("use_percent")) or 0,
        reverse=True,
    )
    full_filesystems = sum(
        1 for item in ranked_filesystems if (report_number(item.get("use_percent")) or 0) >= 80
    )
    lines.extend([
        "",
        f"📁 Filesystems: {len(filesystems)} · ≥80%: {full_filesystems}",
        "Топ за заповненням:",
    ])
    for item in ranked_filesystems[:5]:
        percent = report_number(item.get("use_percent")) or 0
        icon = "🔴" if percent >= 90 else "🟡" if percent >= 80 else "🟢"
        lines.append(
            f"{icon} {item.get('mountpoint') or '?'} · {percent:.0f}% · "
            f"{item.get('available') or '-'} free"
        )
    if not ranked_filesystems:
        lines.append("⚪ Дані недоступні")

    ranked_allocations = sorted(
        [item for item in node_storage if report_number(item.get("usage_percent")) is not None],
        key=lambda item: report_number(item.get("usage_percent")) or 0,
        reverse=True,
    )
    if ranked_allocations:
        lines.extend(["", f"📦 Node allocations: {len(ranked_allocations)}"])
        for item in ranked_allocations[:5]:
            percent = report_number(item.get("usage_percent")) or 0
            icon = "🔴" if percent >= 95 else "🟡" if percent >= 80 else "🟢"
            lines.append(
                f"{icon} {item.get('mountpoint') or item.get('name') or '?'} · "
                f"{percent:.0f}% · {item.get('type') or 'node'}"
            )

    connected_drives = [drive for drive in drives if drive.get("connected") is not False]
    historical_drives = len(drives) - len(connected_drives)
    problem_drives = [
        drive for drive in connected_drives
        if int(drive.get("severity") or 0) >= 2
        or str(drive.get("status") or "").lower() in ("warning", "critical")
    ]
    unknown_drives = [
        drive for drive in connected_drives
        if drive not in problem_drives and drive.get("status") != "ok"
    ]
    lines.extend([
        "",
        f"💽 SMART: {len(connected_drives)} connected · "
        f"проблем {len(problem_drives)} · unknown {len(unknown_drives)} · "
        f"historical {historical_drives}",
    ])
    for drive in problem_drives[:6]:
        name = drive.get("device") or drive.get("name") or "?"
        reasons = ", ".join(drive.get("reasons") or [])
        detail = reasons or f"status={drive.get('status') or 'unknown'}"
        lines.append(
            f"   {report_status_icon(drive.get('status') or 'unknown')} "
            f"{name} · {detail}"
        )
    for drive in unknown_drives[:6]:
        name = drive.get("device") or drive.get("name") or "?"
        reasons = ", ".join(drive.get("reasons") or [])
        detail = reasons or "SMART дані недоступні"
        lines.append(f"   ⚪ {name} · unknown · {detail}")

    if issues and recommendations:
        lines.extend(["", "💡 Рекомендації"])
        for item in recommendations[:3]:
            lines.append(f"• {item}")

    lines.extend(["", "Деталі: /status · /space · /smart · /nas"])

    return "\n".join(lines).strip()


def build_storage_report() -> str:
    return build_storage_report_from_data(fetch_storage_status())


def build_daily_storage_report() -> str:
    data = fetch_storage_status()
    return "\n\n".join([
        build_storage_report_from_data(data),
        "\n".join(zfs_table_lines(data)),
        "\n".join(smart_table_lines(data)),
    ])


def build_smart_report() -> str:
    data = fetch_storage_status()

    lines = [
        "💾 SMART / disk health",
        f"Час: {data.get('checked_at', '-')}",
        "",
    ]
    lines.extend(smart_table_lines(data))

    return "\n".join(lines).strip()


def build_storage_problems_report() -> str:
    data = fetch_storage_status()
    return build_storage_problems_report_from_data(data)


def build_storage_problems_report_from_data(data: dict) -> str:
    issues = data.get("issues", [])

    lines = [
        "🚨 Storage problems",
        f"Стан: {data.get('status', 'unknown').upper()}",
        f"Час: {data.get('checked_at', '-')}",
        "",
    ]

    if not issues:
        lines.append("Проблем не знайдено.")
        return "\n".join(lines).strip()

    for issue in issues[:30]:
        action = issue.get("action")
        lines.append(
            f"- {issue.get('status', 'unknown')}: "
            f"{issue.get('area', '-')}: {issue.get('message', '-')}"
        )
        if action:
            lines.append(f"  Дія: {action}")

    if len(issues) > 30:
        lines.append(f"...ще {len(issues) - 30} проблем")

    return "\n".join(lines).strip()


def build_critical_storage_report_from_data(data: dict) -> str:
    issues = critical_storage_issues(data)

    lines = [
        "🚨 Critical storage alerts",
        f"Стан: {data.get('status', 'unknown').upper()}",
        f"Час: {data.get('checked_at', '-')}",
        "",
    ]

    if not issues:
        lines.append("Critical проблем не знайдено.")
        return "\n".join(lines).strip()

    for issue in issues[:20]:
        action = issue.get("action")
        lines.append(
            f"- {issue.get('area', '-')}: {issue.get('message', '-')}"
        )
        if action:
            lines.append(f"  Дія: {action}")

    if len(issues) > 20:
        lines.append(f"...ще {len(issues) - 20} critical проблем")

    return "\n".join(lines).strip()


def build_space_report() -> str:
    data = fetch_storage_status()
    pools = [
        pool for pool in data.get("pools", [])
        if pool.get("capacity_percent") is not None and pool["capacity_percent"] >= WARN_POOL_CAP
    ]
    filesystems = [
        fs for fs in data.get("filesystems", [])
        if fs.get("use_percent") is not None and fs["use_percent"] >= 80
    ]
    node_storage = [
        item for item in data.get("node_storage", [])
        if item.get("usage_percent") is not None and item["usage_percent"] >= 80
    ]

    lines = [
        "📦 Storage fullness",
        f"Час: {data.get('checked_at', '-')}",
        "",
    ]

    if pools:
        lines.append("Pools >= warn threshold:")
        for pool in pools[:20]:
            lines.append(
                f"- {pool.get('name')}: {pool.get('capacity_percent')}% "
                f"used, free={pool.get('free')}, health={pool.get('health')}"
            )
    else:
        lines.append("Pools >= warn threshold не знайдено.")

    if filesystems:
        lines.extend(["", "Filesystems >=80%:"])
        for fs in filesystems[:30]:
            lines.append(
                f"- {fs.get('mountpoint')}: {fs.get('use_percent')}% "
                f"used, free={fs.get('available')}, fs={fs.get('filesystem')}"
            )

    if node_storage:
        lines.extend(["", "Node allocations >=80%:"])
        for item in node_storage[:30]:
            lines.append(
                f"- {item.get('mountpoint') or item.get('name')}: "
                f"{item.get('usage_percent')}% ({item.get('type', 'node')})"
            )

    lines.extend(["", *zfs_table_lines(data)])

    return "\n".join(lines).strip()


def is_allowed(update: Update) -> bool:
    if not update.effective_chat:
        return False

    if not ALLOWED_CHAT_ID:
        return False

    return str(update.effective_chat.id) == str(ALLOWED_CHAT_ID)


async def send_long_message(context: ContextTypes.DEFAULT_TYPE, chat_id: int | str, text: str):
    for chunk in split_message(text):
        await context.bot.send_message(chat_id=chat_id, text=chunk)


async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id

    text = (
        "Привіт.\n\n"
        "Твій Telegram chat_id:\n\n"
        f"{chat_id}\n\n"
        "Впиши його в telegram-bot/.env:\n\n"
        f"ALLOWED_CHAT_ID={chat_id}\n"
    )

    await update.message.reply_text(text)


async def status_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_allowed(update):
        await update.message.reply_text("⛔ Немає доступу. Спочатку /start і додай chat_id в .env.")
        return

    await update.message.reply_text("Перевіряю storage проблеми...")
    try:
        report = await asyncio.to_thread(build_storage_problems_report)
    except Exception as e:
        report = f"⚠️ storage status error: {e}"

    await send_long_message(context, update.effective_chat.id, report)


async def problems_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_allowed(update):
        await update.message.reply_text("⛔ Немає доступу.")
        return

    await update.message.reply_text("Збираю список проблем...")
    try:
        report = await asyncio.to_thread(build_storage_problems_report)
    except Exception as e:
        report = f"⚠️ storage problems error: {e}"

    await send_long_message(context, update.effective_chat.id, report)


async def zfs_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_allowed(update):
        await update.message.reply_text("⛔ Немає доступу.")
        return

    report = await asyncio.to_thread(build_status_report)
    await send_long_message(context, update.effective_chat.id, report)


async def pools_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_allowed(update):
        await update.message.reply_text("⛔ Немає доступу.")
        return

    report = await asyncio.to_thread(build_pools_report)
    await send_long_message(context, update.effective_chat.id, report)


async def datasets_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_allowed(update):
        await update.message.reply_text("⛔ Немає доступу.")
        return

    report = await asyncio.to_thread(build_datasets_report)
    await send_long_message(context, update.effective_chat.id, report)


async def raw_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_allowed(update):
        await update.message.reply_text("⛔ Немає доступу.")
        return

    report = await asyncio.to_thread(build_raw_report)
    await send_long_message(context, update.effective_chat.id, report)


async def storage_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_allowed(update):
        await update.message.reply_text("⛔ Немає доступу.")
        return

    await update.message.reply_text("Збираю storage status...")
    try:
        report = await asyncio.to_thread(build_storage_report)
    except Exception as e:
        report = f"⚠️ storage status error: {e}"

    await send_long_message(context, update.effective_chat.id, report)


async def smart_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_allowed(update):
        await update.message.reply_text("⛔ Немає доступу.")
        return

    await update.message.reply_text("Збираю SMART/disk health...")
    try:
        report = await asyncio.to_thread(build_smart_report)
    except Exception as e:
        report = f"⚠️ SMART status error: {e}"

    await send_long_message(context, update.effective_chat.id, report)


async def space_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_allowed(update):
        await update.message.reply_text("⛔ Немає доступу.")
        return

    await update.message.reply_text("Збираю fullness по pools/filesystems/nodes...")
    try:
        report = await asyncio.to_thread(build_space_report)
    except Exception as e:
        report = f"⚠️ storage fullness error: {e}"

    await send_long_message(context, update.effective_chat.id, report)


async def nas_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_allowed(update):
        await update.message.reply_text("⛔ Немає доступу.")
        return

    await update.message.reply_text("Перевіряю NAS mount...")
    try:
        report = await asyncio.to_thread(build_nas_report)
    except Exception as e:
        report = f"⚠️ NAS status error: {e}"

    await send_long_message(context, update.effective_chat.id, report)


async def intro_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_allowed(update):
        await update.message.reply_text("⛔ Немає доступу.")
        return

    await update.message.reply_text("Збираю ZFS дані і роблю AI intro...")
    report = await asyncio.to_thread(build_intro_report)
    await send_long_message(context, update.effective_chat.id, report)


async def ask_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_allowed(update):
        await update.message.reply_text("⛔ Немає доступу.")
        return

    question = " ".join(context.args).strip()

    if not question:
        await update.message.reply_text(
            "Напиши питання після команди.\n\n"
            "Приклад:\n"
            "/ask що таке zpool scrub?"
        )
        return

    await update.message.reply_text("Питаю Ollama...")
    answer = await asyncio.to_thread(ask_ollama, question, False)

    await send_long_message(
        context,
        update.effective_chat.id,
        f"🤖 Ollama:\n\n{answer}",
    )


async def askzfs_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_allowed(update):
        await update.message.reply_text("⛔ Немає доступу.")
        return

    question = " ".join(context.args).strip()

    if not question:
        await update.message.reply_text(
            "Напиши питання після команди.\n\n"
            "Приклад:\n"
            "/askzfs який пул найближче до переповнення?"
        )
        return

    await update.message.reply_text("Збираю ZFS статус і питаю Ollama...")
    answer = await asyncio.to_thread(ask_ollama, question, True)

    await send_long_message(
        context,
        update.effective_chat.id,
        f"🤖 Ollama + ZFS context:\n\n{answer}",
    )


async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (
        "Команди:\n\n"
        "/status — тільки storage проблеми\n"
        "/problems — alias для /status\n"
        "/storage — читабельний storage/Sia/Storj/FS звіт\n"
        "/space — переповненість pools/filesystems/nodes\n"
        "/smart — SMART/disk health\n"
        "/nas — NAS mount status для backup\n"
        "/ask питання — спитати Ollama\n"
        "\n"
        "ZFS-only:\n"
        "/zfs — legacy ZFS статус\n"
        "/pools — ZFS pools\n"
        "/datasets — ZFS datasets\n"
        "/raw — сирі zpool/zfs дані\n"
        "/intro — AI короткий висновок по ZFS\n"
        "/askzfs питання — спитати Ollama з ZFS контекстом\n"
        "\n"
        "/start — показати chat_id\n"
        "/help — допомога"
    )

    await update.message.reply_text(text)


async def maybe_send_nas_alert(context: ContextTypes.DEFAULT_TYPE):
    if not ALLOWED_CHAT_ID or not NAS_MONITOR_ENABLED:
        return

    statuses = await asyncio.to_thread(get_nas_statuses)
    issues = nas_alert_issues(statuses)
    state = load_state()

    if not issues:
        if state.get("last_nas_alert_signature"):
            state["last_nas_alert_signature"] = ""
            state["last_nas_alert_time"] = ""
            save_state(state)

            await send_long_message(
                context,
                ALLOWED_CHAT_ID,
                "✅ NAS mount restored\n\n" + build_nas_report(statuses),
            )

        return

    sig = nas_alert_signature(issues)

    if unchanged_alert_in_cooldown(
        state,
        sig,
        signature_key="last_nas_alert_signature",
        time_key="last_nas_alert_time",
        cooldown_hours=NAS_ALERT_UNCHANGED_COOLDOWN_HOURS,
    ):
        return

    state["last_nas_alert_signature"] = sig
    state["last_nas_alert_time"] = now_text()
    save_state(state)

    await send_long_message(
        context,
        ALLOWED_CHAT_ID,
        (
            "🚨 NAS backup mount problem\n\n"
            f"{build_nas_report(statuses)}\n\n"
            "Backup scripts should skip while this is unresolved."
        ),
    )

async def alert_job(context: ContextTypes.DEFAULT_TYPE):
    if not ALLOWED_CHAT_ID:
        return

    await maybe_send_nas_alert(context)

    try:
        data = await asyncio.to_thread(fetch_storage_status)
    except Exception as e:
        state = load_state()
        error_sig = hashlib.sha256(f"storage-status-error:{e}".encode("utf-8")).hexdigest()

        if unchanged_alert_in_cooldown(state, error_sig):
            return

        state["last_storage_problem_signature"] = error_sig
        state["last_storage_problem_time"] = now_text()
        save_state(state)

        await send_long_message(
            context,
            ALLOWED_CHAT_ID,
            (
                "🚨 Storage monitor не зміг зібрати статус\n\n"
                f"URL: {STORAGE_STATUS_URL}\n"
                f"🕒 {now_text()}\n"
                f"Помилка: {e}"
            ),
        )
        return

    issues = critical_storage_issues(data)

    state = load_state()

    if not issues:
        if state.get("last_storage_problem_signature"):
            state["last_storage_problem_signature"] = ""
            state["last_storage_problem_time"] = ""
            save_state(state)

            ok_text = (
                "✅ Critical storage alerts cleared\n\n"
                f"Стан: {data.get('status', 'unknown').upper()}\n"
                f"Час: {data.get('checked_at', now_text())}\n"
                f"{data.get('summary', '')}"
            )

            await send_long_message(context, ALLOWED_CHAT_ID, ok_text)

        return

    sig = critical_storage_signature(data)

    if unchanged_alert_in_cooldown(state, sig):
        return

    state["last_storage_problem_signature"] = sig
    state["last_storage_problem_time"] = now_text()
    save_state(state)

    report = build_critical_storage_report_from_data(data)

    alert_text = (
        "🚨 Є critical проблема зі storage/filesystem/nodes\n\n"
        f"{report}"
    )

    await send_long_message(context, ALLOWED_CHAT_ID, alert_text)


async def daily_summary_job(context: ContextTypes.DEFAULT_TYPE):
    if not ALLOWED_CHAT_ID:
        return

    try:
        report = await asyncio.to_thread(build_daily_storage_report)
    except Exception as e:
        report = f"⚠️ storage daily summary error: {e}"

    await send_long_message(context, ALLOWED_CHAT_ID, report)


def main():
    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN is empty. Set BOT_TOKEN in .env")

    app = Application.builder().token(BOT_TOKEN).concurrent_updates(True).build()

    app.add_handler(CommandHandler("start", start_cmd))
    app.add_handler(CommandHandler("help", help_cmd))
    app.add_handler(CommandHandler("status", status_cmd))
    app.add_handler(CommandHandler("health", status_cmd))
    app.add_handler(CommandHandler("problems", problems_cmd))
    app.add_handler(CommandHandler("zfs", zfs_cmd))
    app.add_handler(CommandHandler("pools", pools_cmd))
    app.add_handler(CommandHandler("datasets", datasets_cmd))
    app.add_handler(CommandHandler("storage", storage_cmd))
    app.add_handler(CommandHandler("smart", smart_cmd))
    app.add_handler(CommandHandler("disks", smart_cmd))
    app.add_handler(CommandHandler("nas", nas_cmd))
    app.add_handler(CommandHandler("space", space_cmd))
    app.add_handler(CommandHandler("raw", raw_cmd))
    app.add_handler(CommandHandler("intro", intro_cmd))
    app.add_handler(CommandHandler("ask", ask_cmd))
    app.add_handler(CommandHandler("askzfs", askzfs_cmd))

    if ALERT_ENABLED and ALLOWED_CHAT_ID:
        app.job_queue.run_repeating(
            alert_job,
            interval=timedelta(minutes=ALERT_INTERVAL_MINUTES),
            first=30,
            name="storage_problem_alerts",
        )
        print(f"Problem alerts enabled every {ALERT_INTERVAL_MINUTES} minutes")
    else:
        print("Problem alerts disabled")

    if DAILY_SUMMARY_ENABLED and ALLOWED_CHAT_ID:
        hour, minute = DAILY_TIME.split(":")

        app.job_queue.run_daily(
            daily_summary_job,
            time=dt_time(
                hour=int(hour),
                minute=int(minute),
                tzinfo=ZoneInfo(TIMEZONE),
            ),
            name="daily_storage_summary",
        )
        print(f"Daily summary enabled at {DAILY_TIME} {TIMEZONE}")
    else:
        print("Daily summary disabled")

    print("Storage Telegram bot started")
    app.run_polling()


if __name__ == "__main__":
    main()
