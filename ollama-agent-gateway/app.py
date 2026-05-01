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

WEB_TRUSTED_DOMAINS = [
    item.strip().lower()
    for item in os.getenv("WEB_TRUSTED_DOMAINS", "").split(",")
    if item.strip()
]

DEFAULT_TIMEZONE = os.getenv("DEFAULT_TIMEZONE", "UTC").strip()
LANGUAGE = os.getenv("LANGUAGE", "uk").strip().lower()

app = FastAPI(title="Ollama Smart Web Agent Gateway")


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

        if usage is not None and usage >= 90:
            add_issue(issues, 3, node_type, msg(f"{mountpoint} ({node_type}) заповнений на {usage:.0f}%.", f"{mountpoint} ({node_type}) is {usage:.0f}% full."), msg("Звільни місце або зменш allocation для node.", "Free space or reduce node allocation."))
        elif usage is not None and usage >= 80:
            add_issue(issues, 2, node_type, msg(f"{mountpoint} ({node_type}) заповнений на {usage:.0f}%.", f"{mountpoint} ({node_type}) is {usage:.0f}% full."), msg("Заплануй розвантаження dataset.", "Plan dataset rebalancing or cleanup."))

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


async def web_zfs_advisor_answer(question: str) -> str:
    web_context = await build_smart_web_context(question)
    zfs_context = await collect_zfs_context()

    prompt = f"""
Питання користувача:
{question}

WEB CONTEXT:
Це результати пошуку + текст відкритих сторінок:

{web_context}

LOCAL ZFS CONTEXT:
Це поточний стан ZFS/дисків з MCP tools:

{zfs_context}

Ти маєш відповісти НЕ просто що змінилось у релізі, а що це означає для МОГО ZFS/fileserver.

Завдання:
- {language_instruction()}
- Спирайся на web context і local ZFS context.
- Не вигадуй фактів, яких немає в джерелах або локальному контексті.
- Якщо в web context є тільки версія пакета, прямо скажи, що деталей мало.
- Дай 5–7 практичних пунктів.
- Окремо напиши: що корисно, що ризиково, що перевірити перед оновленням.
- Якщо бачиш, що всі пули ONLINE, врахуй це.
- Не радь оновлювати production/fileserver без backup/snapshot/scrub/check.
- Для фактів з вебу став номер джерела: [1], [2], [3].

Формат:

🟢/🟡/🔴 Висновок: ...

Що нового для ZFS:
1. ...

Що це значить для твого сервера:
1. ...
2. ...

Перед оновленням перевір:
1. ...

Моя порада:
...
""".strip()

    return await plain_ollama_answer(prompt, temperature=0.1)




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
