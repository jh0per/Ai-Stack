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

DAILY_SUMMARY_ENABLED = os.getenv("DAILY_SUMMARY_ENABLED", "1").strip() == "1"
DAILY_TIME = os.getenv("DAILY_TIME", "09:00").strip()

WARN_POOL_CAP = float(os.getenv("WARN_POOL_CAP", "80"))
CRIT_POOL_CAP = float(os.getenv("CRIT_POOL_CAP", "90"))
MIN_POOL_FREE_GB = float(os.getenv("MIN_POOL_FREE_GB", "500"))
WARN_DATASET_CAP = float(os.getenv("WARN_DATASET_CAP", "90"))

AI_ENABLED = os.getenv("AI_ENABLED", "0").strip() == "1"
OLLAMA_URL = os.getenv("OLLAMA_URL", "http://127.0.0.1:11434/api/chat").strip()
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "qwen2.5:3b").strip()

ZPOOL = os.getenv("ZPOOL_BIN", "/usr/sbin/zpool").strip()
ZFS = os.getenv("ZFS_BIN", "/usr/sbin/zfs").strip()
STORAGE_STATUS_URL = os.getenv(
    "STORAGE_STATUS_URL",
    "http://127.0.0.1:3700/storage/status",
).strip()


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


def build_storage_report() -> str:
    data = fetch_storage_status()
    issues = data.get("issues", [])
    recommendations = data.get("recommendations", [])
    nodes = data.get("nodes", [])
    drives = data.get("drives", [])
    pools = data.get("pools", [])

    lines = [
        "🧭 Storage status",
        f"Стан: {data.get('status', 'unknown').upper()}",
        f"Час: {data.get('checked_at', '-')}",
        "",
        data.get("summary", ""),
        "",
        f"Nodes OK: {sum(1 for n in nodes if n.get('status') == 'ok')}/{len(nodes)}",
        f"Pools ONLINE: {sum(1 for p in pools if p.get('health') == 'ONLINE')}/{len(pools)}",
        f"SMART drives: {len(drives)}",
    ]

    if issues:
        lines.extend(["", "Проблеми:"])
        for issue in issues[:12]:
            lines.append(
                f"- {issue.get('status', 'unknown')}: "
                f"{issue.get('area', '-')}: {issue.get('message', '-')}"
            )
    else:
        lines.extend(["", "Проблем не знайдено."])

    if recommendations:
        lines.extend(["", "Дії:"])
        for item in recommendations[:6]:
            lines.append(f"- {item}")

    return "\n".join(lines).strip()


def build_smart_report() -> str:
    data = fetch_storage_status()
    drives = data.get("drives", [])

    lines = [
        "💾 SMART / disk health",
        f"Час: {data.get('checked_at', '-')}",
        f"Disks: {len(drives)}",
        "",
    ]

    if not drives:
        lines.append("SMART диски не знайдені або gateway не повернув drive дані.")
        return "\n".join(lines).strip()

    problem_drives = [
        d for d in drives
        if d.get("status") not in ("ok", None) or d.get("reallocated") or d.get("pending") or d.get("uncorrectable")
    ]

    if not problem_drives:
        lines.append("Явних SMART проблем не знайдено в доступних даних.")
        lines.append("Якщо всі диски unknown, перевір права smartctl/sudo.")
    else:
        lines.append("Проблемні/невідомі диски:")

    for drive in (problem_drives or drives)[:24]:
        reasons = ", ".join(drive.get("reasons", [])) or "-"
        model = drive.get("model") or "-"
        serial = drive.get("serial") or "-"
        lines.append(
            f"- {drive.get('name')}: {drive.get('status')} | "
            f"{model} | serial {serial} | "
            f"realloc={drive.get('reallocated')} "
            f"pending={drive.get('pending')} "
            f"uncorr={drive.get('uncorrectable')} | {reasons}"
        )

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

    report = await asyncio.to_thread(build_status_report)
    await send_long_message(context, update.effective_chat.id, report)


async def problems_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_allowed(update):
        await update.message.reply_text("⛔ Немає доступу.")
        return

    report = await asyncio.to_thread(build_problems_report)
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
        "/status — короткий статус: ок або проблеми\n"
        "/problems — тільки проблеми\n"
        "/pools — список пулів\n"
        "/datasets — найбільші datasets\n"
        "/storage — повний storage/Sia/Storj/FS статус\n"
        "/smart — SMART/disk health\n"
        "/raw — сирі zpool/zfs дані\n"
        "/intro — AI короткий висновок по ZFS\n"
        "/ask питання — спитати Ollama\n"
        "/askzfs питання — спитати Ollama з ZFS контекстом\n"
        "/start — показати chat_id\n"
        "/help — допомога"
    )

    await update.message.reply_text(text)


async def alert_job(context: ContextTypes.DEFAULT_TYPE):
    if not ALLOWED_CHAT_ID:
        return

    data = await asyncio.to_thread(analyze_zfs)
    problems: list[Problem] = data["problems"]

    state = load_state()

    if not problems:
        if state.get("last_problem_signature"):
            state["last_problem_signature"] = ""
            state["last_problem_time"] = ""
            save_state(state)

            ok_text = (
                "✅ ZFS знову без проблем\n\n"
                f"🖥 Host: {data['hostname']}\n"
                f"🕒 {data['time']}"
            )

            await send_long_message(context, ALLOWED_CHAT_ID, ok_text)

        return

    sig = problem_signature_from_data(data)

    if ALERT_ONLY_ON_CHANGE and state.get("last_problem_signature") == sig:
        return

    state["last_problem_signature"] = sig
    state["last_problem_time"] = now_text()
    save_state(state)

    report = build_problems_report(data)

    alert_text = (
        "🚨 Чувак, є проблема з ZFS\n\n"
        f"{report}"
    )

    await send_long_message(context, ALLOWED_CHAT_ID, alert_text)


async def daily_summary_job(context: ContextTypes.DEFAULT_TYPE):
    if not ALLOWED_CHAT_ID:
        return

    report = await asyncio.to_thread(build_status_report)
    await send_long_message(context, ALLOWED_CHAT_ID, report)


def main():
    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN is empty. Set BOT_TOKEN in .env")

    app = Application.builder().token(BOT_TOKEN).concurrent_updates(True).build()

    app.add_handler(CommandHandler("start", start_cmd))
    app.add_handler(CommandHandler("help", help_cmd))
    app.add_handler(CommandHandler("status", status_cmd))
    app.add_handler(CommandHandler("problems", problems_cmd))
    app.add_handler(CommandHandler("pools", pools_cmd))
    app.add_handler(CommandHandler("datasets", datasets_cmd))
    app.add_handler(CommandHandler("storage", storage_cmd))
    app.add_handler(CommandHandler("smart", smart_cmd))
    app.add_handler(CommandHandler("raw", raw_cmd))
    app.add_handler(CommandHandler("intro", intro_cmd))
    app.add_handler(CommandHandler("ask", ask_cmd))
    app.add_handler(CommandHandler("askzfs", askzfs_cmd))

    if ALERT_ENABLED and ALLOWED_CHAT_ID:
        app.job_queue.run_repeating(
            alert_job,
            interval=timedelta(minutes=ALERT_INTERVAL_MINUTES),
            first=30,
            name="zfs_problem_alerts",
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
            name="daily_zfs_summary",
        )
        print(f"Daily summary enabled at {DAILY_TIME} {TIMEZONE}")
    else:
        print("Daily summary disabled")

    print("ZFS Telegram bot started")
    app.run_polling()


if __name__ == "__main__":
    main()
