import json
import os
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Optional
from urllib.parse import urlparse
from zoneinfo import ZoneInfo

import httpx
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from fastmcp import Client
from pydantic import BaseModel


ROOT_DIR = Path(__file__).resolve().parents[1]
load_dotenv(ROOT_DIR / ".env")


OLLAMA_URL = os.getenv("OLLAMA_URL", "http://127.0.0.1:11434/api/chat").strip()
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "ua-tech").strip()

MCP_URL = os.getenv("MCP_URL", "http://127.0.0.1:3600/mcp").strip()

MCP_ALLOWED_TOOLS = [
    item.strip()
    for item in os.getenv(
        "MCP_ALLOWED_TOOLS",
        "zpool_list,zpool_status,zpool_status_x,zfs_list,disk_free,block_devices_json,disk_identity_json,disk_health_attributes_json,storage_usage_json,node_storage_usage_json,smart_devices_json,docker_containers_json,system_services,list_allowed_dir,read_text_file",
    ).split(",")
    if item.strip()
]

WEB_SEARCH_ENABLED = os.getenv("WEB_SEARCH_ENABLED", "0").strip() == "1"
SEARXNG_URL = os.getenv("SEARXNG_URL", "http://127.0.0.1:8888/search").strip()
WEB_MAX_RESULTS = int(os.getenv("WEB_MAX_RESULTS", "8"))

SMART_WEB_DEFAULT = os.getenv("SMART_WEB_DEFAULT", "1").strip() == "1"
WEB_FETCH_TOP_N = int(os.getenv("WEB_FETCH_TOP_N", "3"))
WEB_FETCH_MAX_CHARS_PER_PAGE = int(os.getenv("WEB_FETCH_MAX_CHARS_PER_PAGE", "7000"))

NODE_STORAGE_WARN_CAP = float(os.getenv("NODE_STORAGE_WARN_CAP", "95"))
NODE_STORAGE_CRIT_CAP = float(os.getenv("NODE_STORAGE_CRIT_CAP", "0"))

WEB_TRUSTED_DOMAINS = [
    item.strip().lower()
    for item in os.getenv("WEB_TRUSTED_DOMAINS", "").split(",")
    if item.strip()
]

DEFAULT_TIMEZONE = os.getenv("DEFAULT_TIMEZONE", "UTC").strip()
LANGUAGE = os.getenv("LANGUAGE", "uk").strip().lower()

app = FastAPI(title="Ollama Smart Web Agent Gateway")


CHAT_HTML = """
<!doctype html>
<html lang="uk">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Ollama Chat</title>
  <style>
    :root {
      color-scheme: light dark;
      --bg: #f5f7f8;
      --panel: #ffffff;
      --text: #182026;
      --muted: #66737f;
      --border: #d8e0e6;
      --accent: #0f766e;
      --accent-text: #ffffff;
      --user: #e6f5f3;
      --assistant: #ffffff;
      --error: #b42318;
    }

    @media (prefers-color-scheme: dark) {
      :root {
        --bg: #111416;
        --panel: #181d20;
        --text: #edf2f4;
        --muted: #a2adb7;
        --border: #2d373d;
        --accent: #2dd4bf;
        --accent-text: #062320;
        --user: #123632;
        --assistant: #1f262a;
        --error: #ffb4ab;
      }
    }

    * {
      box-sizing: border-box;
    }

    body {
      margin: 0;
      min-height: 100vh;
      background: var(--bg);
      color: var(--text);
      font-family: system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
    }

    .app {
      width: min(980px, 100%);
      min-height: 100vh;
      margin: 0 auto;
      display: grid;
      grid-template-rows: auto 1fr auto;
      background: var(--panel);
      border-left: 1px solid var(--border);
      border-right: 1px solid var(--border);
    }

    header {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 16px;
      padding: 14px 18px;
      border-bottom: 1px solid var(--border);
    }

    h1 {
      margin: 0;
      font-size: 18px;
      line-height: 1.2;
      font-weight: 650;
      letter-spacing: 0;
    }

    .status {
      color: var(--muted);
      font-size: 13px;
      white-space: nowrap;
    }

    main {
      overflow-y: auto;
      padding: 18px;
    }

    .messages {
      display: flex;
      flex-direction: column;
      gap: 12px;
    }

    .message {
      max-width: 86%;
      padding: 12px 14px;
      border: 1px solid var(--border);
      border-radius: 8px;
      line-height: 1.45;
      white-space: pre-wrap;
      overflow-wrap: anywhere;
    }

    .message.user {
      align-self: flex-end;
      background: var(--user);
    }

    .message.assistant {
      align-self: flex-start;
      background: var(--assistant);
    }

    .message.error {
      color: var(--error);
    }

    form {
      display: grid;
      grid-template-columns: 1fr auto;
      gap: 10px;
      padding: 14px 18px 18px;
      border-top: 1px solid var(--border);
      background: var(--panel);
    }

    .controls {
      grid-column: 1 / -1;
      display: flex;
      align-items: center;
      gap: 10px;
      flex-wrap: wrap;
    }

    select,
    textarea,
    button {
      font: inherit;
    }

    select {
      min-height: 36px;
      border: 1px solid var(--border);
      border-radius: 8px;
      background: var(--panel);
      color: var(--text);
      padding: 0 10px;
    }

    textarea {
      width: 100%;
      min-height: 52px;
      max-height: 220px;
      resize: vertical;
      border: 1px solid var(--border);
      border-radius: 8px;
      background: var(--panel);
      color: var(--text);
      padding: 12px;
      line-height: 1.4;
    }

    button {
      min-height: 52px;
      border: 0;
      border-radius: 8px;
      background: var(--accent);
      color: var(--accent-text);
      padding: 0 18px;
      font-weight: 650;
      cursor: pointer;
    }

    button:disabled {
      cursor: wait;
      opacity: 0.65;
    }

    @media (max-width: 640px) {
      .app {
        border: 0;
      }

      header {
        align-items: flex-start;
        flex-direction: column;
        gap: 6px;
      }

      main {
        padding: 12px;
      }

      .message {
        max-width: 94%;
      }

      form {
        grid-template-columns: 1fr;
        padding: 12px;
      }

      button {
        width: 100%;
      }
    }
  </style>
</head>
<body>
  <div class="app">
    <header>
      <h1>Ollama Chat</h1>
      <div class="status" id="status">checking gateway...</div>
    </header>

    <main id="scroll">
      <div class="messages" id="messages"></div>
    </main>

    <form id="chat-form">
      <div class="controls">
        <select id="endpoint" aria-label="Chat mode">
          <option value="/agent">Agent</option>
          <option value="/ask">Ollama only</option>
          <option value="/agent-web">Web assisted</option>
          <option value="/agent-zfs">ZFS/storage</option>
        </select>
      </div>
      <textarea id="question" name="question" placeholder="Type a message..." autocomplete="off" required></textarea>
      <button id="send" type="submit">Send</button>
    </form>
  </div>

  <script>
    const form = document.getElementById("chat-form");
    const question = document.getElementById("question");
    const endpoint = document.getElementById("endpoint");
    const messages = document.getElementById("messages");
    const scroll = document.getElementById("scroll");
    const statusEl = document.getElementById("status");
    const send = document.getElementById("send");

    function addMessage(role, text, isError = false) {
      const item = document.createElement("div");
      item.className = `message ${role}${isError ? " error" : ""}`;
      item.textContent = text;
      messages.appendChild(item);
      scroll.scrollTop = scroll.scrollHeight;
      return item;
    }

    async function refreshHealth() {
      try {
        const response = await fetch("/health");
        const data = await response.json();
        statusEl.textContent = `${data.ollama_model} · ${data.status}`;
      } catch (error) {
        statusEl.textContent = "gateway unavailable";
      }
    }

    form.addEventListener("submit", async (event) => {
      event.preventDefault();

      const text = question.value.trim();
      if (!text) {
        return;
      }

      addMessage("user", text);
      question.value = "";
      question.focus();
      send.disabled = true;

      const pending = addMessage("assistant", "Thinking...");

      try {
        const response = await fetch(endpoint.value, {
          method: "POST",
          headers: {"Content-Type": "application/json"},
          body: JSON.stringify({question: text, temperature: 0.2}),
        });

        const data = await response.json();
        pending.textContent = data.answer || `HTTP ${response.status}`;
        if (!response.ok) {
          pending.classList.add("error");
        }
      } catch (error) {
        pending.textContent = `Request failed: ${error}`;
        pending.classList.add("error");
      } finally {
        send.disabled = false;
        scroll.scrollTop = scroll.scrollHeight;
      }
    });

    question.addEventListener("keydown", (event) => {
      if (event.key === "Enter" && !event.shiftKey) {
        event.preventDefault();
        form.requestSubmit();
      }
    });

    addMessage("assistant", "Ask Ollama here. Use Agent for routed local help, Ollama only for plain chat, Web assisted when SearXNG is enabled, or ZFS/storage for storage questions.");
    refreshHealth();
  </script>
</body>
</html>
""".strip()


class AskRequest(BaseModel):
    question: str = ""
    temperature: Optional[float] = 0.2


class AskResponse(BaseModel):
    answer: str


class StorageStatusResponse(BaseModel):
    status: str
    severity: int
    summary: str
    checked_at: str
    issues: list[dict[str, Any]]
    recommendations: list[str]
    nodes: list[dict[str, Any]]
    pools: list[dict[str, Any]]
    datasets: list[dict[str, Any]]
    node_storage: list[dict[str, Any]]
    filesystems: list[dict[str, Any]]
    drives: list[dict[str, Any]]
    raw_available: list[str]


def normalize_mcp_result(result: Any) -> str:
    if result is None:
        return ""

    if hasattr(result, "content"):
        parts = []
        for item in result.content:
            if hasattr(item, "text"):
                parts.append(item.text)
            else:
                parts.append(str(item))
        return "\n".join(parts).strip()

    return str(result)


async def ollama_chat(messages: list[dict], temperature: float = 0.2) -> dict:
    payload = {
        "model": OLLAMA_MODEL,
        "stream": False,
        "messages": messages,
        "options": {
            "temperature": temperature,
            "num_ctx": 8192,
        },
    }

    async with httpx.AsyncClient(timeout=240) as client:
        response = await client.post(OLLAMA_URL, json=payload)

        if response.status_code >= 400:
            raise RuntimeError(f"Ollama HTTP {response.status_code}: {response.text}")

        return response.json()


async def plain_ollama_answer(question: str, temperature: float = 0.2) -> str:
    messages = [
        {
            "role": "system",
            "content": (
                "You are a local technical assistant. "
                f"{language_instruction()} "
                "Do not use Russian."
            ),
        },
        {
            "role": "user",
            "content": question,
        },
    ]

    data = await ollama_chat(messages, temperature=temperature)
    return data.get("message", {}).get("content", "").strip()


async def mcp_list_tools_raw() -> list[Any]:
    async with Client(MCP_URL) as client:
        tools = await client.list_tools()
        return list(tools)


async def mcp_call_tool(tool_name: str, arguments: Optional[dict] = None) -> str:
    if tool_name not in MCP_ALLOWED_TOOLS:
        return f"Tool '{tool_name}' is blocked."

    arguments = arguments or {}

    async with Client(MCP_URL) as client:
        result = await client.call_tool(tool_name, arguments)
        return normalize_mcp_result(result)


async def current_time(timezone: str = "") -> str:
    tz_name = timezone.strip() or DEFAULT_TIMEZONE

    try:
        now = datetime.now(ZoneInfo(tz_name))
    except Exception:
        tz_name = DEFAULT_TIMEZONE
        now = datetime.now(ZoneInfo(tz_name))

    return now.strftime(f"%Y-%m-%d %H:%M:%S {tz_name}")


def safe_json_loads(text: str) -> dict[str, Any]:
    try:
        data = json.loads(text)
        if isinstance(data, dict):
            return data
    except Exception:
        pass

    return {"ok": False, "raw": text}


def severity_status(severity: int) -> str:
    if severity >= 3:
        return "critical"
    if severity == 2:
        return "warning"
    if severity == 1:
        return "unknown"
    return "ok"


def use_english() -> bool:
    return LANGUAGE in ("en", "eng", "english")


def language_name() -> str:
    return "English" if use_english() else "Ukrainian"


def language_instruction() -> str:
    if use_english():
        return "Answer in English. Be concise and practical. Do not invent facts."
    return "Відповідай українською, коротко і практично. Не вигадуй фактів."


def msg(uk: str, en: str) -> str:
    return en if use_english() else uk


def add_issue(
    issues: list[dict[str, Any]],
    severity: int,
    area: str,
    message: str,
    action: str = "",
) -> None:
    issues.append({
        "severity": severity,
        "status": severity_status(severity),
        "area": area,
        "message": message,
        "action": action,
    })


def parse_percent(value: str) -> Optional[float]:
    match = re.search(r"(\d+(?:\.\d+)?)%", value or "")
    if not match:
        return None
    return float(match.group(1))


def parse_zpool_list(text: str) -> list[dict[str, Any]]:
    lines = [line for line in text.splitlines() if line.strip()]
    if len(lines) < 2:
        return []

    pools = []
    headers = re.split(r"\s+", lines[0].strip())

    for line in lines[1:]:
        if line.startswith("STDERR:") or line.startswith("EXIT_CODE="):
            continue

        values = re.split(r"\s+", line.strip())
        if len(values) < len(headers):
            continue

        item = dict(zip(headers, values))
        cap = parse_percent(item.get("CAP", ""))
        pools.append({
            "name": item.get("NAME", ""),
            "size": item.get("SIZE", ""),
            "allocated": item.get("ALLOC", ""),
            "free": item.get("FREE", ""),
            "capacity_percent": cap,
            "fragmentation": item.get("FRAG", ""),
            "health": item.get("HEALTH", ""),
            "raw": item,
        })

    return pools


def parse_zfs_list(text: str) -> list[dict[str, Any]]:
    lines = [line for line in text.splitlines() if line.strip()]
    if len(lines) < 2:
        return []

    datasets = []
    headers = re.split(r"\s+", lines[0].strip())

    for line in lines[1:]:
        if line.startswith("STDERR:") or line.startswith("EXIT_CODE="):
            continue

        values = re.split(r"\s+", line.strip(), maxsplit=len(headers) - 1)
        if len(values) < len(headers):
            continue

        item = dict(zip(headers, values))
        datasets.append({
            "name": item.get("NAME", ""),
            "used": item.get("USED", ""),
            "available": item.get("AVAIL", ""),
            "referenced": item.get("REFER", ""),
            "quota": item.get("QUOTA", ""),
            "refquota": item.get("REFQUOTA", ""),
            "mountpoint": item.get("MOUNTPOINT", ""),
            "raw": item,
        })

    return datasets


def parse_df(text: str) -> list[dict[str, Any]]:
    filesystems = []

    for line in text.splitlines()[1:]:
        if not line.strip() or line.startswith("STDERR:") or line.startswith("EXIT_CODE="):
            continue

        values = re.split(r"\s+", line.strip(), maxsplit=5)
        if len(values) != 6:
            continue

        filesystems.append({
            "filesystem": values[0],
            "size": values[1],
            "used": values[2],
            "available": values[3],
            "use_percent": parse_percent(values[4]),
            "mountpoint": values[5],
        })

    return filesystems


def service_line_to_node(line: str) -> Optional[dict[str, Any]]:
    low = line.lower()
    if not any(word in low for word in ("storj", "storagenode", "sia", "siad", "hostd", "renterd", "walletd")):
        return None

    values = re.split(r"\s+", line.strip(), maxsplit=4)
    if len(values) < 4:
        return None

    name = values[0]
    load = values[1]
    active = values[2]
    sub = values[3]
    description = values[4] if len(values) > 4 else ""
    severity = 0 if active == "active" and sub in ("running", "exited") else 3

    return {
        "type": "systemd",
        "name": name,
        "project": detect_node_project(name + " " + description),
        "status": severity_status(severity),
        "severity": severity,
        "state": active,
        "substate": sub,
        "load": load,
        "description": description,
    }


def detect_node_project(text: str) -> str:
    low = text.lower()
    if "storj" in low or "storagenode" in low:
        return "storj"
    if any(word in low for word in ("sia", "siad", "hostd", "renterd", "walletd")):
        return "sia"
    return "unknown"


def extract_nodes(docker_text: str, services_text: str) -> list[dict[str, Any]]:
    nodes = []
    docker_data = safe_json_loads(docker_text)

    for item in docker_data.get("containers", []) or []:
        haystack = " ".join(str(item.get(key, "")) for key in ("Names", "Image", "Command", "Labels"))
        project = detect_node_project(haystack)
        if project == "unknown":
            continue

        state = (item.get("State") or "").lower()
        status_text = (item.get("Status") or "").lower()
        severity = 0

        if state != "running":
            severity = 3
        elif "unhealthy" in status_text:
            severity = 3
        elif "health: starting" in status_text:
            severity = 2

        nodes.append({
            "type": "docker",
            "project": project,
            "name": item.get("Names", ""),
            "image": item.get("Image", ""),
            "status": severity_status(severity),
            "severity": severity,
            "state": item.get("State", ""),
            "runtime_status": item.get("Status", ""),
            "ports": item.get("Ports", ""),
            "mounts": item.get("Mounts", ""),
        })

    for line in services_text.splitlines():
        node = service_line_to_node(line)
        if node:
            nodes.append(node)

    return nodes


def smart_attr(raw: dict[str, Any], names: set[str]) -> Optional[int]:
    table = raw.get("ata_smart_attributes", {}).get("table", []) or []

    for item in table:
        name = str(item.get("name", "")).lower()
        if name in names:
            raw_value = item.get("raw", {}).get("value")
            try:
                return int(raw_value)
            except Exception:
                return None

    return None


def extract_smart_drives(smart_text: str) -> list[dict[str, Any]]:
    data = safe_json_loads(smart_text)
    drives = []

    for item in data.get("devices", []) or []:
        result = item.get("result", {})
        raw = result.get("data", {}) if result.get("ok") else {}
        passed = raw.get("smart_status", {}).get("passed")
        temp = raw.get("temperature", {}).get("current")
        reallocated = smart_attr(raw, {"reallocated_sector_ct", "reallocated_event_count"})
        pending = smart_attr(raw, {"current_pending_sector"})
        uncorrectable = smart_attr(raw, {"offline_uncorrectable", "reported_uncorrect"})

        severity = 0
        reasons = []

        if passed is False:
            severity = max(severity, 3)
            reasons.append("SMART health не пройдено")
        if pending and pending > 0:
            severity = max(severity, 3)
            reasons.append(f"pending sectors: {pending}")
        if uncorrectable and uncorrectable > 0:
            severity = max(severity, 3)
            reasons.append(f"uncorrectable errors: {uncorrectable}")
        if reallocated and reallocated > 0:
            severity = max(severity, 2)
            reasons.append(f"reallocated sectors/events: {reallocated}")
        if isinstance(temp, int) and temp >= 55:
            severity = max(severity, 3)
            reasons.append(f"temperature: {temp}C")
        elif isinstance(temp, int) and temp >= 45:
            severity = max(severity, 2)
            reasons.append(f"temperature: {temp}C")
        if not result.get("ok"):
            severity = max(severity, 1)
            reasons.append("SMART дані не прочитані")

        drives.append({
            "name": item.get("name", ""),
            "type": item.get("type"),
            "model": raw.get("model_name") or raw.get("device", {}).get("model_name", ""),
            "serial": raw.get("serial_number", ""),
            "status": severity_status(severity),
            "severity": severity,
            "smart_passed": passed,
            "temperature_c": temp,
            "power_on_hours": raw.get("power_on_time", {}).get("hours"),
            "reallocated": reallocated,
            "pending": pending,
            "uncorrectable": uncorrectable,
            "reasons": reasons,
        })

    return drives


def extract_disk_identities(identity_text: str) -> dict[str, dict[str, Any]]:
    data = safe_json_loads(identity_text)
    output = {}

    for disk in data.get("disks", []) or []:
        name = disk.get("name")
        if name:
            output[name] = disk

    return output


def extract_disk_health_attributes(health_text: str) -> dict[str, dict[str, Any]]:
    data = safe_json_loads(health_text)
    output = {}

    for disk in data.get("disks", []) or []:
        name = disk.get("name")
        if not name:
            continue

        attrs = {}
        for attr in disk.get("attributes", []) or []:
            try:
                raw_value = int(attr.get("raw", 0))
            except Exception:
                raw_value = None
            attrs[int(attr.get("id", 0))] = {**attr, "raw_int": raw_value}

        output[name] = {
            "ok": disk.get("ok"),
            "attributes": attrs,
            "raw": disk.get("raw", ""),
        }

    return output


def extract_node_storage_usage(text: str) -> list[dict[str, Any]]:
    data = safe_json_loads(text)
    return list(data.get("datasets", []) or [])


def merge_disk_tool_data(
    drives: list[dict[str, Any]],
    identities: dict[str, dict[str, Any]],
    health_attrs: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    by_name = {drive["name"]: drive for drive in drives if drive.get("name")}

    for name, identity in identities.items():
        drive = by_name.setdefault(name, {
            "name": name,
            "type": None,
            "model": "",
            "serial": "",
            "status": "unknown",
            "severity": 1,
            "smart_passed": None,
            "temperature_c": None,
            "power_on_hours": None,
            "reallocated": None,
            "pending": None,
            "uncorrectable": None,
            "reasons": [],
        })
        drive["model"] = drive.get("model") or identity.get("model", "")
        drive["serial"] = drive.get("serial") or identity.get("serial", "")

    for name, health in health_attrs.items():
        drive = by_name.setdefault(name, {
            "name": name,
            "type": None,
            "model": "",
            "serial": "",
            "status": "unknown",
            "severity": 1,
            "smart_passed": None,
            "temperature_c": None,
            "power_on_hours": None,
            "reallocated": None,
            "pending": None,
            "uncorrectable": None,
            "reasons": [],
        })

        attrs = health.get("attributes", {})
        reallocated = attrs.get(5, {}).get("raw_int")
        pending = attrs.get(197, {}).get("raw_int")
        uncorrectable = attrs.get(198, {}).get("raw_int")

        if reallocated is not None:
            drive["reallocated"] = reallocated
        if pending is not None:
            drive["pending"] = pending
        if uncorrectable is not None:
            drive["uncorrectable"] = uncorrectable

        reasons = list(drive.get("reasons", []))
        severity = int(drive.get("severity", 0) or 0)

        if pending and pending > 0:
            severity = max(severity, 3)
            reasons.append(f"pending sectors: {pending}")
        if uncorrectable and uncorrectable > 0:
            severity = max(severity, 3)
            reasons.append(f"offline uncorrectable: {uncorrectable}")
        if reallocated and reallocated > 0:
            severity = max(severity, 2)
            reasons.append(f"reallocated sectors: {reallocated}")

        drive["severity"] = severity
        drive["status"] = severity_status(severity)
        drive["reasons"] = list(dict.fromkeys(reasons))

    return list(by_name.values())


async def collect_storage_raw() -> dict[str, str]:
    tools = [
        "zpool_list",
        "zpool_status_x",
        "zpool_status",
        "zfs_list",
        "disk_free",
        "block_devices_json",
        "disk_identity_json",
        "disk_health_attributes_json",
        "storage_usage_json",
        "node_storage_usage_json",
        "smart_devices_json",
        "docker_containers_json",
        "system_services",
    ]
    raw = {}

    for tool_name in tools:
        if tool_name not in MCP_ALLOWED_TOOLS:
            continue
        raw[tool_name] = await mcp_call_tool(tool_name, {})

    return raw


def analyze_storage(raw: dict[str, str], checked_at: str) -> StorageStatusResponse:
    issues: list[dict[str, Any]] = []
    recommendations: list[str] = []
    pools = parse_zpool_list(raw.get("zpool_list", ""))
    datasets = parse_zfs_list(raw.get("zfs_list", ""))
    filesystems = parse_df(raw.get("disk_free", ""))
    drives = merge_disk_tool_data(
        extract_smart_drives(raw.get("smart_devices_json", "")),
        extract_disk_identities(raw.get("disk_identity_json", "")),
        extract_disk_health_attributes(raw.get("disk_health_attributes_json", "")),
    )
    node_storage = extract_node_storage_usage(
        raw.get("storage_usage_json") or raw.get("node_storage_usage_json", "")
    )
    node_storage_mounts = {
        item.get("mountpoint")
        for item in node_storage
        if item.get("mountpoint")
    }
    nodes = extract_nodes(raw.get("docker_containers_json", ""), raw.get("system_services", ""))
    zpool_status_x = raw.get("zpool_status_x", "")

    for pool in pools:
        name = pool["name"]
        health = pool["health"]
        cap = pool["capacity_percent"]

        if health and health != "ONLINE":
            add_issue(issues, 3, "zpool", msg(f"Pool {name} має health={health}.", f"Pool {name} has health={health}."), msg("Перевір zpool status і заміни/віднови проблемний диск.", "Check zpool status and replace or repair the affected disk."))
        if cap is not None and cap >= 90:
            add_issue(issues, 3, "zpool", msg(f"Pool {name} заповнений на {cap:.0f}%.", f"Pool {name} is {cap:.0f}% full."), msg("Звільни місце або розшир pool.", "Free space or expand the pool."))
        elif cap is not None and cap >= 80:
            add_issue(issues, 2, "zpool", msg(f"Pool {name} заповнений на {cap:.0f}%.", f"Pool {name} is {cap:.0f}% full."), msg("Заплануй очищення або розширення.", "Plan cleanup or expansion."))

    if zpool_status_x and "all pools are healthy" not in zpool_status_x.lower():
        if any(word in zpool_status_x.upper() for word in ("DEGRADED", "FAULTED", "OFFLINE", "UNAVAIL")):
            add_issue(issues, 3, "zpool", msg("zpool status -x показує проблеми.", "zpool status -x reports problems."), msg("Відкрий деталі zpool status.", "Inspect full zpool status."))
        elif "EXIT_CODE=" not in zpool_status_x:
            add_issue(issues, 2, "zpool", msg("zpool status -x не повернув чистий healthy стан.", "zpool status -x did not return a clean healthy state."), msg("Перевір повний zpool status.", "Check full zpool status."))

    for fs in filesystems:
        use_percent = fs["use_percent"]
        mountpoint = fs["mountpoint"]

        if mountpoint in node_storage_mounts:
            continue

        if use_percent is not None and use_percent >= 90:
            add_issue(issues, 3, "space", msg(f"{mountpoint} заповнений на {use_percent:.0f}%.", f"{mountpoint} is {use_percent:.0f}% full."), msg("Звільни місце або перенеси дані.", "Free space or move data."))
        elif use_percent is not None and use_percent >= 80:
            add_issue(issues, 2, "space", msg(f"{mountpoint} заповнений на {use_percent:.0f}%.", f"{mountpoint} is {use_percent:.0f}% full."), msg("Тримай запас місця для storage node і файлової системи.", "Keep free space for the storage node and filesystem."))

    for dataset in node_storage:
        usage = dataset.get("usage_percent")
        mountpoint = dataset.get("mountpoint") or dataset.get("name")
        node_type = dataset.get("type", "node")

        if usage is None:
            continue

        if NODE_STORAGE_CRIT_CAP > 0 and usage >= NODE_STORAGE_CRIT_CAP:
            add_issue(
                issues,
                3,
                node_type,
                msg(
                    f"{mountpoint} ({node_type}) allocation заповнений на {usage:.0f}%.",
                    f"{mountpoint} ({node_type}) allocation is {usage:.0f}% full.",
                ),
                msg(
                    "Перевір, чи це очікувана зайнятість rented storage; якщо node починає помилятися, зменш allocation або додай місце.",
                    "Check whether this is expected rented storage usage; if the node starts failing, reduce allocation or add space.",
                ),
            )
        elif usage >= NODE_STORAGE_WARN_CAP:
            add_issue(
                issues,
                2,
                node_type,
                msg(
                    f"{mountpoint} ({node_type}) allocation заповнений на {usage:.0f}%.",
                    f"{mountpoint} ({node_type}) allocation is {usage:.0f}% full.",
                ),
                msg(
                    "Це може бути нормально для Sia/Storj, але тримай під моніторингом logs і фактичний вільний простір.",
                    "This can be normal for Sia/Storj, but monitor logs and actual free space.",
                ),
            )

    for drive in drives:
        if drive["severity"] >= 2:
            add_issue(
                issues,
                drive["severity"],
                "smart",
                msg(f"{drive['name']} {drive.get('model', '')} має SMART статус {drive['status']}: {', '.join(drive['reasons'])}.", f"{drive['name']} {drive.get('model', '')} has SMART status {drive['status']}: {', '.join(drive['reasons'])}."),
                msg("Перевір диск, backup, scrub і план заміни якщо лічильники ростуть.", "Check the disk, backup, filesystem scrub/check, and replacement plan if counters grow."),
            )

    for node in nodes:
        if node["severity"] >= 2:
            add_issue(
                issues,
                node["severity"],
                node["project"],
                msg(f"{node['project']} node {node['name']} має стан {node['status']} ({node.get('runtime_status') or node.get('state')}).", f"{node['project']} node {node['name']} has status {node['status']} ({node.get('runtime_status') or node.get('state')})."),
                msg("Перевір logs, healthcheck, мережу, порти і доступність storage path.", "Check logs, healthcheck, network, ports, and storage path availability."),
            )

    found_projects = {node["project"] for node in nodes}
    for project in ("sia", "storj"):
        if project not in found_projects:
            add_issue(issues, 1, project, msg(f"{project} node не знайдено серед Docker/systemd.", f"{project} node was not found in Docker/systemd."), msg("Якщо node запускається іншим способом, додай її service/container name або інтеграцію.", "If it runs another way, add its service/container name or integration."))

    if "smart_devices_json" not in raw:
        add_issue(issues, 1, "smart", msg("SMART tool не дозволений у MCP_ALLOWED_TOOLS.", "SMART tool is not allowed in MCP_ALLOWED_TOOLS."), msg("Додай smart_devices_json в MCP_ALLOWED_TOOLS.", "Add smart_devices_json to MCP_ALLOWED_TOOLS."))
    elif not drives:
        add_issue(issues, 1, "smart", msg("SMART диски не знайдені або smartctl не прочитав дані.", "SMART disks were not found or smartctl could not read data."), msg("Перевір smartmontools і права доступу.", "Check smartmontools and permissions."))

    if "docker_containers_json" not in raw and "system_services" not in raw:
        add_issue(issues, 1, "nodes", msg("Немає джерел для пошуку Sia/Storj нод.", "No source is available for Sia/Storj node discovery."), msg("Дозволь docker_containers_json або system_services.", "Allow docker_containers_json or system_services."))

    severity = max([issue["severity"] for issue in issues], default=0)
    status = severity_status(severity)

    if severity >= 3:
        recommendations.append(msg("Спочатку усунь critical проблеми: pool/SMART/місце/node down.", "Fix critical issues first: pool/SMART/space/node down."))
    if any(issue["area"] == "smart" for issue in issues):
        recommendations.append(msg("Після SMART попереджень зроби backup важливих даних і перевірку файлової системи.", "After SMART warnings, back up important data and run filesystem checks."))
    if any(issue["area"] == "space" for issue in issues):
        recommendations.append(msg("Для storage node тримай запас місця, бо переповнення ламає роботу нод і файлових систем.", "Keep free space for storage nodes because full filesystems break node operation."))
    if any(issue["area"] in ("sia", "storj") for issue in issues):
        recommendations.append(msg("Для Sia/Storj висока зайнятість allocation може бути нормальною; критичним вважай node errors, 100% filesystem або нестачу реального вільного місця.", "For Sia/Storj, high allocation usage can be normal; treat node errors, a 100% filesystem, or lack of real free space as critical."))
    if not recommendations:
        recommendations.append(msg("Критичних проблем не видно. Продовжуй регулярні filesystem checks/scrub, SMART моніторинг і перевірку node logs.", "No critical issues detected. Continue regular filesystem checks/scrub, SMART monitoring, and node log review."))

    healthy_pools = sum(1 for pool in pools if pool.get("health") == "ONLINE")
    running_nodes = sum(1 for node in nodes if node.get("status") == "ok")
    summary = (
        f"{msg('Статус', 'Status')}: {status}. Pools ONLINE: {healthy_pools}/{len(pools)}. "
        f"Nodes running: {running_nodes}/{len(nodes)}. SMART drives: {len(drives)}. "
        f"Issues: {len(issues)}."
    )

    return StorageStatusResponse(
        status=status,
        severity=severity,
        summary=summary,
        checked_at=checked_at,
        issues=sorted(issues, key=lambda item: item["severity"], reverse=True),
        recommendations=recommendations,
        nodes=nodes,
        pools=pools,
        datasets=datasets,
        node_storage=node_storage,
        filesystems=filesystems,
        drives=drives,
        raw_available=sorted(raw.keys()),
    )


async def storage_status_data() -> StorageStatusResponse:
    checked_at = await current_time(DEFAULT_TIMEZONE)
    raw = await collect_storage_raw()
    return analyze_storage(raw, checked_at)


def domain_of(url: str) -> str:
    try:
        return urlparse(url).netloc.lower().removeprefix("www.")
    except Exception:
        return ""


def domain_is_trusted(url: str) -> bool:
    domain = domain_of(url)

    for trusted in WEB_TRUSTED_DOMAINS:
        if domain == trusted or domain.endswith("." + trusted):
            return True

    return False


def tokenize(text: str) -> set[str]:
    return set(
        word.lower()
        for word in re.findall(r"[a-zA-Zа-яА-ЯіІїЇєЄґҐ0-9]{3,}", text)
    )


def score_search_result(item: dict, query: str) -> int:
    title = item.get("title", "") or ""
    content = item.get("content", "") or ""
    url = item.get("url", "") or ""

    score = 0

    if domain_is_trusted(url):
        score += 50

    q_tokens = tokenize(query)
    text_tokens = tokenize(title + " " + content + " " + url)
    score += len(q_tokens & text_tokens) * 5

    lower_url = url.lower()
    lower_title = title.lower()

    if "documentation" in lower_url or "docs" in lower_url:
        score += 15

    if "release" in lower_title or "release-notes" in lower_url:
        score += 10

    if "github.com" in lower_url:
        score += 8

    if "wiki" in lower_url:
        score -= 5

    return score


def build_search_queries(question: str) -> list[str]:
    q = question.strip()
    low = q.lower()

    queries = [q]

    if "ubuntu" in low:
        queries.append(f"{q} official release notes")
        queries.append(f"{q} site:documentation.ubuntu.com")
        queries.append(f"{q} site:discourse.ubuntu.com")

    if "zfs" in low or "openzfs" in low or "scrub" in low:
        queries.append(f"{q} OpenZFS documentation")
        queries.append(f"{q} site:openzfs.github.io")
        queries.append(f"{q} site:github.com/openzfs")

    if "docker" in low:
        queries.append(f"{q} official Docker docs")
        queries.append(f"{q} site:docs.docker.com")

    if "python" in low:
        queries.append(f"{q} official Python docs")
        queries.append(f"{q} site:docs.python.org")

    return list(dict.fromkeys(queries))


async def web_search_results(query: str) -> list[dict]:
    if not WEB_SEARCH_ENABLED:
        return []

    all_results: list[dict] = []
    seen_urls: set[str] = set()

    for search_query in build_search_queries(query):
        params = {
            "q": search_query,
            "format": "json",
        }

        try:
            async with httpx.AsyncClient(timeout=30) as client:
                response = await client.get(SEARXNG_URL, params=params)
                response.raise_for_status()
                data = response.json()
        except Exception:
            continue

        for item in data.get("results", []):
            url = (item.get("url") or "").strip()

            if not url or url in seen_urls:
                continue

            seen_urls.add(url)

            result = {
                "title": (item.get("title") or "").strip(),
                "url": url,
                "content": (item.get("content") or "").strip(),
                "score": score_search_result(item, query),
            }

            all_results.append(result)

    all_results.sort(key=lambda x: x["score"], reverse=True)
    return all_results[:WEB_MAX_RESULTS]


async def web_search(query: str) -> str:
    results = await web_search_results(query)

    if not results:
        return "No web results found."

    lines = []

    for index, item in enumerate(results, start=1):
        lines.append(f"[{index}] {item['title']}")
        lines.append(f"URL: {item['url']}")

        if item["content"]:
            lines.append(f"Snippet: {item['content']}")

        lines.append(f"Score: {item['score']}")
        lines.append("")

    return "\n".join(lines).strip()


async def fetch_page_text(url: str) -> str:
    parsed = urlparse(url)

    if parsed.scheme not in ("http", "https"):
        return "Unsupported URL scheme."

    headers = {
        "User-Agent": "Mozilla/5.0 compatible local-ai-agent/1.0"
    }

    try:
        async with httpx.AsyncClient(
            timeout=30,
            follow_redirects=True,
            headers=headers,
        ) as client:
            response = await client.get(url)
            response.raise_for_status()

            content_type = response.headers.get("content-type", "").lower()

            if "text/html" not in content_type:
                return f"Non-HTML content: {content_type}"

            html = response.text

    except Exception as e:
        return f"Fetch error: {e}"

    soup = BeautifulSoup(html, "lxml")

    for tag in soup(["script", "style", "nav", "footer", "header", "aside", "form", "noscript"]):
        tag.decompose()

    title = ""
    if soup.title and soup.title.string:
        title = soup.title.string.strip()

    meta_desc = ""
    meta = soup.find("meta", attrs={"name": "description"})
    if meta and meta.get("content"):
        meta_desc = meta.get("content").strip()

    chunks = []

    if title:
        chunks.append(f"TITLE: {title}")

    if meta_desc:
        chunks.append(f"DESCRIPTION: {meta_desc}")

    main = soup.find("main") or soup.find("article") or soup.body or soup

    for tag in main.find_all(["h1", "h2", "h3", "p", "li", "pre", "code"]):
        text = " ".join(tag.get_text(" ", strip=True).split())

        if len(text) < 25:
            continue

        chunks.append(text)

        if sum(len(c) for c in chunks) >= WEB_FETCH_MAX_CHARS_PER_PAGE:
            break

    text = "\n".join(chunks).strip()

    if not text:
        return "No readable text extracted."

    return text[:WEB_FETCH_MAX_CHARS_PER_PAGE]


async def build_smart_web_context(question: str) -> str:
    results = await web_search_results(question)

    if not results:
        return "No search results."

    selected = results[:WEB_FETCH_TOP_N]

    parts = []

    for index, item in enumerate(selected, start=1):
        title = item["title"]
        url = item["url"]
        snippet = item["content"]
        score = item["score"]

        fetched_text = await fetch_page_text(url)

        parts.append("=" * 80)
        parts.append(f"SOURCE [{index}]")
        parts.append(f"Title: {title}")
        parts.append(f"URL: {url}")
        parts.append(f"Domain: {domain_of(url)}")
        parts.append(f"Trusted: {domain_is_trusted(url)}")
        parts.append(f"Score: {score}")

        if snippet:
            parts.append(f"Search snippet: {snippet}")

        parts.append("")
        parts.append("Fetched page text:")
        parts.append(fetched_text)
        parts.append("")

    return "\n".join(parts)


async def smart_web_augmented_answer(question: str) -> str:
    context = await build_smart_web_context(question)

    prompt = f"""
Питання користувача:
{question}

Нижче web context. Це результати пошуку + текст відкритих сторінок:

{context}

Завдання:
- {language_instruction()}
- Спирайся тільки на надані джерела.
- Якщо даних недостатньо — прямо скажи.
- Якщо джерела суперечать одне одному — скажи про це.
- Для фактів став номер джерела: [1], [2], [3].
- Не вигадуй.
- Дай коротко і практично.
""".strip()

    return await plain_ollama_answer(prompt, temperature=0.1)


async def web_augmented_answer(question: str) -> str:
    return await smart_web_augmented_answer(question)


def looks_like_update_question(question: str) -> bool:
    q = question.lower()
    words = [
        "update",
        "upgrade",
        "оновлен",
        "оновити",
        "апгрейд",
        "реліз",
        "release",
        "версія",
        "version",
    ]
    return any(w in q for w in words)


def build_storage_advisor_search_query(question: str) -> str:
    q = question.strip()

    if looks_like_update_question(q):
        return q

    return (
        f"{q} OpenZFS zpool status scrub SMART disk health "
        "filesystem capacity best practices documentation"
    )


def compact_storage_status(status: StorageStatusResponse) -> dict[str, Any]:
    filesystems_over_80 = [
        fs for fs in status.filesystems
        if (fs.get("use_percent") is not None and fs["use_percent"] >= 80)
    ]
    node_storage_over_80 = [
        item for item in status.node_storage
        if (item.get("usage_percent") is not None and item["usage_percent"] >= 80)
    ]

    return {
        "status": status.status,
        "severity": status.severity,
        "summary": status.summary,
        "checked_at": status.checked_at,
        "issues": status.issues[:25],
        "recommendations": status.recommendations,
        "pools": [
            {
                "name": pool.get("name"),
                "health": pool.get("health"),
                "capacity_percent": pool.get("capacity_percent"),
                "free": pool.get("free"),
                "fragmentation": pool.get("fragmentation"),
            }
            for pool in status.pools
        ],
        "filesystems_over_80": filesystems_over_80[:25],
        "node_storage_over_80": node_storage_over_80[:25],
        "drives_not_ok": [
            drive for drive in status.drives
            if drive.get("status") != "ok"
        ],
        "nodes_total": len(status.nodes),
        "nodes_not_ok": [
            node for node in status.nodes
            if node.get("status") != "ok"
        ],
        "raw_available": status.raw_available,
    }


def severity_icon(severity: int) -> str:
    if severity >= 3:
        return "🔴"
    if severity >= 2:
        return "🟡"
    return "🟢"


def format_storage_advisor_local_report(status: StorageStatusResponse) -> str:
    pools_online = sum(1 for pool in status.pools if pool.get("health") == "ONLINE")
    pool_count = len(status.pools)
    nodes_ok = sum(1 for node in status.nodes if node.get("status") == "ok")
    pool_warnings = [
        pool for pool in status.pools
        if pool.get("health") != "ONLINE"
        or (pool.get("capacity_percent") is not None and pool["capacity_percent"] >= 80)
    ]
    drive_warnings = [
        drive for drive in status.drives
        if drive.get("status") != "ok"
    ]
    critical_issues = [
        issue for issue in status.issues
        if issue.get("severity", 0) >= 3
    ]
    warning_issues = [
        issue for issue in status.issues
        if issue.get("severity") == 2
    ]

    lines = [
        f"{severity_icon(status.severity)} Висновок: {status.summary} [local]",
        "",
        "Що бачу локально:",
        f"1. ZFS pools ONLINE: {pools_online}/{pool_count}. [local]",
        f"2. Nodes running: {nodes_ok}/{len(status.nodes)}. [local]",
        f"3. SMART drives read: {len(status.drives)}. [local]",
    ]

    if critical_issues:
        lines.append(f"4. Critical issues: {len(critical_issues)}. [local]")
    elif warning_issues:
        lines.append(f"4. Critical issues: 0, warnings: {len(warning_issues)}. [local]")
    else:
        lines.append("4. Явних critical/warning issues у зібраних даних немає. [local]")

    if pool_warnings:
        lines.append("")
        lines.append("Пули, які потребують уваги:")
        for index, pool in enumerate(pool_warnings[:10], start=1):
            lines.append(
                f"{index}. {pool.get('name')}: health={pool.get('health')}, "
                f"cap={pool.get('capacity_percent')}%, free={pool.get('free')}, "
                f"frag={pool.get('fragmentation')}. [local]"
            )

    if critical_issues or warning_issues:
        lines.append("")
        lines.append("Проблеми з локального аналізу:")
        for index, issue in enumerate((critical_issues + warning_issues)[:12], start=1):
            lines.append(
                f"{index}. {issue.get('status')}: {issue.get('area')}: "
                f"{issue.get('message')} Дія: {issue.get('action')} [local]"
            )

    if drive_warnings:
        lines.append("")
        lines.append("SMART / температура:")
        for index, drive in enumerate(drive_warnings[:10], start=1):
            reasons = ", ".join(drive.get("reasons") or [])
            lines.append(
                f"{index}. {drive.get('name')} {drive.get('model')}: "
                f"status={drive.get('status')}, temp={drive.get('temperature_c')}C, "
                f"reallocated={drive.get('reallocated')}, pending={drive.get('pending')}, "
                f"uncorrectable={drive.get('uncorrectable')}; {reasons}. [local]"
            )

    return "\n".join(lines)


def extract_web_source_refs(web_context: str) -> list[str]:
    refs = []
    current_index = ""
    current_title = ""
    current_url = ""

    for line in web_context.splitlines():
        source_match = re.match(r"SOURCE \[(\d+)\]", line.strip())
        if source_match:
            if current_index and current_title:
                refs.append(f"[{current_index}] {current_title} ({current_url})")
            current_index = source_match.group(1)
            current_title = ""
            current_url = ""
            continue

        if line.startswith("Title: "):
            current_title = line.removeprefix("Title: ").strip()
        elif line.startswith("URL: "):
            current_url = line.removeprefix("URL: ").strip()

    if current_index and current_title:
        refs.append(f"[{current_index}] {current_title} ({current_url})")

    return refs[:3]


def format_storage_advisor_actions(
    status: StorageStatusResponse,
    web_context: str,
) -> str:
    critical_issues = [
        issue for issue in status.issues
        if issue.get("severity", 0) >= 3
    ]
    warning_issues = [
        issue for issue in status.issues
        if issue.get("severity") == 2
    ]
    pool_warnings = [
        pool for pool in status.pools
        if pool.get("health") != "ONLINE"
        or (pool.get("capacity_percent") is not None and pool["capacity_percent"] >= 80)
    ]
    drive_warnings = [
        drive for drive in status.drives
        if drive.get("status") != "ok"
    ]
    web_refs = extract_web_source_refs(web_context)
    web_suffix = " " + " ".join(ref.split(" ", 1)[0] for ref in web_refs) if web_refs else ""

    lines = []

    lines.append("Web-довідка:")
    if web_refs:
        for ref in web_refs:
            lines.append(f"- {ref}")
    else:
        lines.append("- Web search не повернув придатних джерел; нижче висновок тільки з локальних даних.")

    lines.append("")
    lines.append("Ризики / прогалини в перевірці:")
    if critical_issues:
        lines.append(
            f"1. Є {len(critical_issues)} critical проблем з вільним місцем у storage datasets; "
            f"це ризик для роботи Sia/Storj і файлових систем.{web_suffix}"
        )
    else:
        lines.append("1. Critical проблем у зібраному локальному статусі не видно. [local]")

    if pool_warnings:
        names = ", ".join(str(pool.get("name")) for pool in pool_warnings[:8])
        lines.append(
            f"2. ZFS pools ONLINE, але частина pools вже >=80% capacity: {names}. "
            f"Для ZFS це зона, де треба планувати cleanup/розширення, а не чекати 90%+.{web_suffix}"
        )
    else:
        lines.append("2. ZFS pools не показують capacity warning у локальному статусі. [local]")

    if drive_warnings:
        names = ", ".join(str(drive.get("name")) for drive in drive_warnings[:8])
        lines.append(
            f"3. Є SMART/temperature warnings на дисках: {names}; ONLINE pool не скасовує ризик деградації диска.{web_suffix}"
        )
    else:
        lines.append("3. SMART/temperature warning по дисках у локальному статусі немає. [local]")

    if warning_issues:
        lines.append(
            f"4. Додатково є {len(warning_issues)} warning issues; їх краще закрити до росту в critical. [local]"
        )
    else:
        lines.append("4. Додаткових warning issues у локальному статусі немає. [local]")

    lines.append("")
    lines.append("Що перевірити зараз:")
    lines.append("1. `zpool status -x` і повний `zpool status`: чи немає read/write/checksum errors, resilver/scrub in progress або degraded vdev.")
    lines.append("2. `zpool list`: прибрати або розширити pools/datasets, які вже >=80%, особливо ті, де storage allocation доходить до 90-98%.")
    lines.append("3. SMART по проблемних дисках: перевірити, чи ростуть reallocated/pending/uncorrectable, і знизити температуру NVMe.")
    lines.append("4. Для Sia/Storj: зменшити allocation або перенести дані з datasets, які вже на 90%+, потім перевірити logs контейнерів.")

    lines.append("")
    lines.append("Моя порада:")
    if status.severity >= 3:
        lines.append(
            "Не називав би стан файлової системи повністю нормальним: ZFS pools онлайн, але статус storage загалом critical через заповнення datasets. "
            "Спершу звільни/перерозподіли місце на critical Sia/Storj mountpoints, потім займайся warning pools і SMART/NVMe температурою."
        )
    elif status.severity == 2:
        lines.append(
            "Стан робочий, але не ідеальний: critical немає, warning треба закрити планово. "
            "Пріоритет: capacity запас, SMART trend, регулярний scrub."
        )
    else:
        lines.append(
            "За зібраними даними стан виглядає нормальним. Продовжуй регулярний scrub, SMART моніторинг і контроль вільного місця."
        )

    return "\n".join(lines)


async def collect_zfs_health_context() -> str:
    wanted_tools = [
        "zpool_list",
        "zpool_status_x",
        "zpool_status",
    ]

    parts = []

    for tool_name in wanted_tools:
        if tool_name not in MCP_ALLOWED_TOOLS:
            continue

        result = await mcp_call_tool(tool_name, {})

        parts.append("=" * 80)
        parts.append(tool_name)
        parts.append("=" * 80)
        parts.append(result[:12000])
        parts.append("")

    if not parts:
        return "No ZFS health MCP tools available."

    return "\n".join(parts)


async def web_zfs_advisor_answer(question: str) -> str:
    web_query = build_storage_advisor_search_query(question)
    web_context = await build_smart_web_context(web_query)
    storage_status = await storage_status_data()
    local_report = format_storage_advisor_local_report(storage_status)
    update_mode = looks_like_update_question(question)

    if not update_mode:
        actions = format_storage_advisor_actions(storage_status, web_context)
        return f"{local_report}\n\n{actions}".strip()

    zfs_context = await collect_zfs_health_context()
    storage_status_json = json.dumps(
        compact_storage_status(storage_status),
        ensure_ascii=False,
        indent=2,
    )
    update_instruction = "Користувач питає про оновлення/версії: оціни користь, ризики і що перевірити перед оновленням."

    prompt = f"""
Питання користувача:
{question}

WEB SEARCH QUERY USED:
{web_query}

WEB CONTEXT:
Це результати пошуку + текст відкритих сторінок. Використовуй їх як інструкції/критерії оцінки, а не як заміну локальним даним:

{web_context}

LOCAL STORAGE SUMMARY:
Це вже розібраний локальний стан storage/filesystems/nodes/drives:

{storage_status_json}

LOCAL FACTS THAT MUST BE PRESERVED EXACTLY:
{local_report}

LOCAL ZFS CONTEXT:
Це сирий поточний стан ZFS/дисків з MCP tools:

{zfs_context}

Ти професійний storage/SRE advisor для локального fileserver.
Спочатку розбери конкретне питання користувача, потім застосуй web context як довідку, і тільки після цього дай висновок по локальних даних.
{update_instruction}

Завдання:
- {language_instruction()}
- Головним джерелом для стану МОГО сервера є LOCAL STORAGE SUMMARY і LOCAL ZFS CONTEXT.
- WEB CONTEXT використовуй для правил оцінки: як читати zpool status, scrub/resilver/errors, SMART, заповнення filesystem/pool.
- Не вигадуй фактів, яких немає в web sources або локальному контексті.
- Якщо web context порожній або слабкий, прямо скажи і все одно дай локальний висновок з обмеженнями.
- Якщо локальних даних для частини питання немає, напиши що саме не перевірено.
- Якщо всі пули ONLINE, це добре, але не називай це повною гарантією без SMART/scrub/error counters.
- Для фактів з локального стану посилайся як [local].
- Для фактів з вебу став номер джерела: [1], [2], [3].
- Не зводь відповідь до загальних backup-порад; спершу назви конкретні знайдені стани, проблеми і ризики.
- Не додавай нові локальні факти поза LOCAL FACTS THAT MUST BE PRESERVED EXACTLY.
- Не повторюй секції "Висновок", "Що бачу локально", "Пули, які потребують уваги", "Проблеми з локального аналізу", "SMART / температура".

Формат:

Ризики / прогалини в перевірці:
1. ...

Що перевірити зараз:
1. ...

Моя порада:
...
""".strip()

    advice = await plain_ollama_answer(prompt, temperature=0.1)
    return f"{local_report}\n\n{advice}".strip()




async def collect_zfs_context() -> str:
    wanted_tools = [
        "zpool_list",
        "zpool_status_x",
        "zpool_status",
        "zfs_list",
        "disk_free",
    ]

    parts = []

    for tool_name in wanted_tools:
        if tool_name not in MCP_ALLOWED_TOOLS:
            continue

        result = await mcp_call_tool(tool_name, {})

        parts.append("=" * 80)
        parts.append(tool_name)
        parts.append("=" * 80)
        parts.append(result)
        parts.append("")

    if not parts:
        return "No ZFS MCP tools available."

    return "\n".join(parts)


async def zfs_status_answer() -> str:
    context = await collect_zfs_context()

    prompt = f"""
Ось дані ZFS/дисків з MCP tools:

{context}

Зроби короткий статус. Language: {language_name()}.

Формат:
🟢/🟡/🔴 Стан: ...
Головне: ...
Проблеми: ...
Дія: ...

Правила:
- Якщо всі пули ONLINE — скажи це.
- Якщо є DEGRADED/FAULTED/OFFLINE/UNAVAIL — це критично.
- Якщо pool >80% — попередження.
- Якщо pool >90% — критично.
- Не вигадуй.
""".strip()

    return await plain_ollama_answer(prompt, temperature=0.1)


async def storage_status_answer() -> str:
    status = await storage_status_data()

    lines = [
        f"{status.status.upper()}: {status.summary}",
        "",
        msg("Проблеми:", "Issues:"),
    ]

    if status.issues:
        for issue in status.issues[:10]:
            lines.append(f"- {issue['status']}: {issue['area']}: {issue['message']}")
    else:
        lines.append(msg("- Немає явних проблем у зібраних даних.", "- No obvious issues in collected data."))

    lines.append("")
    lines.append(msg("Дії:", "Actions:"))
    for item in status.recommendations:
        lines.append(f"- {item}")

    return "\n".join(lines)


async def zfs_question_answer(question: str) -> str:
    context = await collect_zfs_context()

    prompt = f"""
Ось поточний ZFS/дисковий контекст з MCP tools:

{context}

Питання користувача:
{question}

{language_instruction()}
Не вигадуй того, чого немає в даних.
""".strip()

    return await plain_ollama_answer(prompt, temperature=0.1)


def looks_like_time_question(question: str) -> bool:
    q = question.lower()
    words = [
        "який час",
        "котра година",
        "скільки часу",
        "час зараз",
        "дата зараз",
        "сьогодні",
        "now",
        "current time",
        "time in",
    ]
    return any(w in q for w in words)


def looks_like_zfs_question(question: str) -> bool:
    q = question.lower()
    words = [
        "zfs",
        "zpool",
        "scrub",
        "pool",
        "пул",
        "пули",
        "диск",
        "диски",
        "dataset",
        "quota",
        "refquota",
        "storj",
        "sia",
    ]
    return any(w in q for w in words)


def looks_like_storage_status_question(question: str) -> bool:
    q = question.lower()
    words = [
        "статус сервера",
        "стан сервера",
        "всю інформацію",
        "готову інформацію",
        "хелс",
        "health",
        "smart",
        "смарт",
        "hard",
        "hdd",
        "диск",
        "хар",
        "storj",
        "sia",
        "сія",
        "сторж",
    ]
    return any(w in q for w in words)


def looks_like_web_question(question: str) -> bool:
    q = question.lower()
    words = [
        "знайди",
        "пошукай",
        "актуальн",
        "остання версія",
        "новини",
        "що нового",
        "latest",
        "current",
        "today",
        "web",
        "інтернет",
    ]
    return any(w in q for w in words)


async def routed_agent_answer(question: str, prefer_web: bool = False) -> str:
    question = question.strip()

    if not question:
        return "Питання порожнє."

    if looks_like_time_question(question):
        return await current_time(DEFAULT_TIMEZONE)

    if looks_like_storage_status_question(question) and not looks_like_web_question(question):
        return await storage_status_answer()

    if looks_like_zfs_question(question) and looks_like_web_question(question):
        return await web_zfs_advisor_answer(question)

    if looks_like_zfs_question(question) and not looks_like_web_question(question):
        return await zfs_question_answer(question)

    if prefer_web or SMART_WEB_DEFAULT or looks_like_web_question(question):
        return await smart_web_augmented_answer(question)

    return await plain_ollama_answer(question, temperature=0.2)


@app.get("/", response_class=HTMLResponse)
async def chat_page():
    return HTMLResponse(CHAT_HTML)


@app.get("/health")
async def health():
    return {
        "status": "ok",
        "ollama_model": OLLAMA_MODEL,
        "ollama_url": OLLAMA_URL,
        "mcp_url": MCP_URL,
        "web_search_enabled": WEB_SEARCH_ENABLED,
        "searxng_url": SEARXNG_URL,
        "smart_web_default": SMART_WEB_DEFAULT,
        "web_fetch_top_n": WEB_FETCH_TOP_N,
        "default_timezone": DEFAULT_TIMEZONE,
    }


@app.get("/mcp/tools")
async def mcp_tools():
    tools = await mcp_list_tools_raw()
    output = []

    for tool in tools:
        name = getattr(tool, "name", "")
        description = getattr(tool, "description", "")

        output.append({
            "name": name,
            "description": description,
            "allowed": name in MCP_ALLOWED_TOOLS,
        })

    return output


@app.post("/ask", response_model=AskResponse)
async def ask(req: AskRequest):
    try:
        answer = await plain_ollama_answer(req.question, req.temperature or 0.2)
    except Exception as e:
        answer = f"⚠️ Ollama error: {e}"

    return AskResponse(answer=answer)


@app.post("/ask-web", response_model=AskResponse)
async def ask_web(req: AskRequest):
    try:
        answer = await web_augmented_answer(req.question)
    except Exception as e:
        answer = f"⚠️ ask-web error: {e}"

    return AskResponse(answer=answer)


@app.post("/ask-zfs", response_model=AskResponse)
async def ask_zfs(req: AskRequest):
    try:
        answer = await zfs_question_answer(req.question)
    except Exception as e:
        answer = f"⚠️ ask-zfs error: {e}"

    return AskResponse(answer=answer)


@app.post("/zfs/status", response_model=AskResponse)
async def zfs_status():
    try:
        answer = await zfs_status_answer()
    except Exception as e:
        answer = f"⚠️ zfs/status error: {e}"

    return AskResponse(answer=answer)


@app.get("/storage/status", response_model=StorageStatusResponse)
async def storage_status_get():
    return await storage_status_data()


@app.post("/storage/status", response_model=StorageStatusResponse)
async def storage_status_post():
    return await storage_status_data()


@app.post("/agent", response_model=AskResponse)
async def agent(req: AskRequest):
    try:
        answer = await routed_agent_answer(req.question, prefer_web=False)
    except Exception as e:
        answer = f"⚠️ Agent error: {e}"

    return AskResponse(answer=answer)


@app.post("/agent-zfs", response_model=AskResponse)
async def agent_zfs(req: AskRequest):
    try:
        answer = await zfs_question_answer(req.question)
    except Exception as e:
        answer = f"⚠️ Agent-ZFS error: {e}"

    return AskResponse(answer=answer)


@app.post("/agent-web", response_model=AskResponse)
async def agent_web(req: AskRequest):
    try:
        if looks_like_zfs_question(req.question):
            answer = await web_zfs_advisor_answer(req.question)
        else:
            answer = await smart_web_augmented_answer(req.question)
    except Exception as e:
        answer = f"⚠️ Agent-Web error: {e}"

    return AskResponse(answer=answer)


@app.post("/web/search", response_model=AskResponse)
async def web_search_endpoint(req: AskRequest):
    try:
        answer = await web_search(req.question)
    except Exception as e:
        answer = f"⚠️ web/search error: {e}"

    return AskResponse(answer=answer)


@app.post("/web/context", response_model=AskResponse)
async def web_context_endpoint(req: AskRequest):
    try:
        answer = await build_smart_web_context(req.question)
    except Exception as e:
        answer = f"⚠️ web/context error: {e}"

    return AskResponse(answer=answer)



@app.post("/advisor-zfs-web", response_model=AskResponse)
async def advisor_zfs_web(req: AskRequest):
    try:
        answer = await web_zfs_advisor_answer(req.question)
    except Exception as e:
        answer = f"⚠️ advisor-zfs-web error: {e}"

    return AskResponse(answer=answer)
