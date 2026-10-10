import csv
import json
import os
import subprocess
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv
from fastmcp import FastMCP


ROOT_DIR = Path(__file__).resolve().parents[1]
load_dotenv(ROOT_DIR / ".env")

MCP_HOST = os.getenv("MCP_HOST", "127.0.0.1").strip()
MCP_PORT = int(os.getenv("MCP_PORT", "3600"))

ZPOOL = os.getenv("ZPOOL_BIN", "/usr/sbin/zpool").strip()
ZFS = os.getenv("ZFS_BIN", "/usr/sbin/zfs").strip()
LSBLK = os.getenv("LSBLK_BIN", "/usr/bin/lsblk").strip()
FINDMNT = os.getenv("FINDMNT_BIN", "/usr/bin/findmnt").strip()
SMARTCTL = os.getenv("SMARTCTL_BIN", "/usr/sbin/smartctl").strip()
SMARTCTL_USE_SUDO = os.getenv("SMARTCTL_USE_SUDO", "0").strip() == "1"
SMART_LOG_DIR = Path(os.getenv("SMART_LOG_DIR", "/var/lib/smartmontools").strip())
DOCKER = os.getenv("DOCKER_BIN", "/usr/bin/docker").strip()
SYSTEMCTL = os.getenv("SYSTEMCTL_BIN", "/usr/bin/systemctl").strip()

FS_ALLOWED_ROOTS = [
    Path(p.strip()).resolve()
    for p in os.getenv("FS_ALLOWED_ROOTS", "/mnt").split(",")
    if p.strip()
]

MAX_READ_FILE_BYTES = int(os.getenv("MAX_READ_FILE_BYTES", "20000"))

mcp = FastMCP("storage-tools")


def run_cmd(cmd: list[str], timeout: int = 60) -> str:
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=timeout,
            check=False,
        )

        output = []

        if result.stdout.strip():
            output.append(result.stdout.strip())

        if result.stderr.strip():
            output.append("STDERR:")
            output.append(result.stderr.strip())

        if result.returncode != 0:
            output.append(f"EXIT_CODE={result.returncode}")

        return "\n".join(output).strip()

    except Exception as e:
        return f"ERROR running {' '.join(cmd)}: {e}"


def command_exists(path: str) -> bool:
    return Path(path).exists() and os.access(path, os.X_OK)


def smartctl_cmd(args: list[str]) -> list[str]:
    if SMARTCTL_USE_SUDO:
        return ["sudo", "-n", SMARTCTL, *args]

    return [SMARTCTL, *args]


def direct_smartctl_cmd(args: list[str]) -> list[str]:
    return [SMARTCTL, *args]


def run_json_cmd(cmd: list[str], timeout: int = 60) -> dict:
    output = run_cmd(cmd, timeout=timeout)

    try:
        return {
            "ok": True,
            "command": cmd,
            "data": json.loads(output),
            "raw": output,
        }
    except Exception:
        return {
            "ok": False,
            "command": cmd,
            "error": "Command did not return valid JSON.",
            "raw": output,
        }


def parse_smart_text_attributes(text: str) -> list[dict]:
    rows = []

    for line in text.splitlines():
        parts = line.split()
        if len(parts) < 10:
            continue

        try:
            attr_id = int(parts[0])
        except Exception:
            continue

        if attr_id not in (5, 197, 198):
            continue

        rows.append({
            "id": attr_id,
            "name": parts[1],
            "value": parts[3] if len(parts) > 3 else "",
            "worst": parts[4] if len(parts) > 4 else "",
            "thresh": parts[5] if len(parts) > 5 else "",
            "raw": parts[-1],
            "line": line,
        })

    return rows


def read_last_line(path: Path) -> str:
    """Read the final non-empty line without loading a multi-megabyte smartd log."""
    with path.open("rb") as handle:
        handle.seek(0, os.SEEK_END)
        position = handle.tell()
        buffer = bytearray()

        while position > 0:
            position -= 1
            handle.seek(position)
            char = handle.read(1)
            if char == b"\n" and buffer:
                break
            if char not in (b"\n", b"\r"):
                buffer.extend(char)

    return bytes(reversed(buffer)).decode("utf-8", errors="replace").strip()


def optional_int(value: str | None) -> int | None:
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return None


def smartd_device_map() -> dict[str, str]:
    if not command_exists(LSBLK):
        return {}

    result = run_json_cmd([LSBLK, "-J", "-d", "-o", "NAME,SERIAL"])
    mapping = {}

    for device in result.get("data", {}).get("blockdevices", []) or []:
        name = str(device.get("name") or "").strip()
        serial = str(device.get("serial") or "").strip()
        if name and serial:
            mapping[serial] = f"/dev/{name}"

    return mapping


def parse_smartd_snapshot(path: Path, devices: dict[str, str]) -> dict | None:
    filename = path.name
    if filename.endswith(".ata.csv"):
        protocol = "ATA"
        stem = filename.removeprefix("attrlog.").removesuffix(".ata.csv")
    elif filename.endswith(".scsi.csv"):
        protocol = "SAS"
        stem = filename.removeprefix("attrlog.").removesuffix(".scsi.csv")
    else:
        return None

    line = read_last_line(path)
    if not line:
        return None

    fields = [value.strip() for value in next(csv.reader([line], delimiter=";"))]
    snapshot_at = fields[0] if fields else ""
    attributes = {}

    if protocol == "ATA":
        for index in range(1, len(fields) - 2, 3):
            attr_id = fields[index]
            raw_value = fields[index + 2]
            if attr_id and raw_value:
                attributes[attr_id] = raw_value

        temperature_raw = optional_int(attributes.get("194") or attributes.get("190"))
        temperature = temperature_raw & 0xFFFF if temperature_raw is not None else None
        hours_raw = optional_int(attributes.get("9"))
        if hours_raw is None:
            power_on_hours = None
        elif hours_raw > 0xFFFFFFFF:
            power_on_hours = hours_raw & 0xFFFF
        else:
            power_on_hours = hours_raw & 0xFFFFFFFF

        reallocated = optional_int(attributes.get("5"))
        pending = optional_int(attributes.get("197"))
        uncorrectable = optional_int(attributes.get("198"))
    else:
        for index in range(1, len(fields) - 1, 2):
            key = fields[index]
            value = fields[index + 1]
            if key and value:
                attributes[key] = value

        temperature = optional_int(attributes.get("temperature"))
        power_on_hours = None
        reallocated = None
        pending = None
        error_values = [
            optional_int(attributes.get(key))
            for key in (
                "read-total-unc-errors",
                "write-total-unc-errors",
                "verify-total-unc-errors",
            )
        ]
        present_errors = [value for value in error_values if value is not None]
        uncorrectable = sum(present_errors) if present_errors else None

    serial = stem.rsplit("-", 1)[-1]
    model = stem.rsplit("-", 1)[0].replace("_", " ") if "-" in stem else stem.replace("_", " ")
    device = devices.get(serial)
    severity = 0
    reasons = []

    if temperature is not None and temperature > 51:
        severity = max(severity, 3)
        reasons.append(f"temperature: {temperature}C")
    elif temperature is not None and temperature > 41:
        severity = max(severity, 2)
        reasons.append(f"temperature: {temperature}C")
    if reallocated is not None and reallocated > 0:
        severity = max(severity, 2)
        reasons.append(f"reallocated sectors/events: {reallocated}")
    if pending is not None and pending > 0:
        severity = max(severity, 3)
        reasons.append(f"pending sectors: {pending}")
    if uncorrectable is not None and uncorrectable > 0:
        severity = max(severity, 3)
        reasons.append(f"uncorrectable errors: {uncorrectable}")

    return {
        "name": device or serial,
        "device": device,
        "connected": device is not None,
        "protocol": protocol,
        "model": model,
        "serial": serial,
        "snapshot_at": snapshot_at,
        "temperature_c": temperature,
        "power_on_hours": power_on_hours,
        "reallocated": reallocated,
        "pending": pending,
        "uncorrectable": uncorrectable,
        "severity": severity,
        "status": "critical" if severity >= 3 else "warning" if severity >= 2 else "ok",
        "reasons": reasons,
    }


def ensure_allowed_path(path: str) -> Path:
    target = Path(path).expanduser().resolve()

    for root in FS_ALLOWED_ROOTS:
        try:
            target.relative_to(root)
            return target
        except ValueError:
            pass

    allowed = ", ".join(str(p) for p in FS_ALLOWED_ROOTS)
    raise ValueError(f"Path is not allowed: {target}. Allowed roots: {allowed}")


@mcp.tool()
def disk_free() -> str:
    """Show disk usage with df -h."""
    return run_cmd(["df", "-h"])


@mcp.tool()
def block_devices_json() -> str:
    """Show block devices, filesystems, sizes, serials and mountpoints as JSON."""
    if not command_exists(LSBLK):
        return json.dumps({
            "ok": False,
            "error": f"lsblk not found or not executable: {LSBLK}",
        })

    result = run_json_cmd([
        LSBLK,
        "-J",
        "-O",
        "-b",
    ])
    return json.dumps(result, ensure_ascii=False)


@mcp.tool()
def disk_identity_json() -> str:
    """List physical disks with model and serial where smartctl can read identity."""
    if not command_exists(LSBLK):
        return json.dumps({
            "ok": False,
            "error": f"lsblk not found or not executable: {LSBLK}",
        })

    stdout = run_cmd([LSBLK, "-S", "-o", "NAME", "-n"], timeout=60)
    disks = []

    for name in stdout.splitlines():
        name = name.strip()
        if not name or name.startswith("STDERR:") or name.startswith("EXIT_CODE="):
            continue

        dev = f"/dev/{name}"
        result = run_cmd(smartctl_cmd(["-i", dev]), timeout=60)
        if ("sudo: a password is required" in result or "a terminal is required" in result) and SMARTCTL_USE_SUDO:
            result = run_cmd(direct_smartctl_cmd(["-i", dev]), timeout=60)

        model = ""
        serial = ""

        for line in result.splitlines():
            if line.startswith("Device Model:") or line.startswith("Model Number:"):
                model = line.split(":", 1)[1].strip()
            elif line.startswith("Serial Number:"):
                serial = line.split(":", 1)[1].strip()

        disks.append({
            "name": dev,
            "model": model,
            "serial": serial,
            "ok": bool(model or serial),
            "raw": result,
        })

    return json.dumps({
        "ok": True,
        "disks": disks,
    }, ensure_ascii=False)


@mcp.tool()
def disk_health_attributes_json() -> str:
    """Return SMART attributes 5, 197 and 198 for physical disks."""
    if not command_exists(LSBLK):
        return json.dumps({
            "ok": False,
            "error": f"lsblk not found or not executable: {LSBLK}",
        })

    stdout = run_cmd([LSBLK, "-S", "-o", "NAME", "-n"], timeout=60)
    disks = []

    for name in stdout.splitlines():
        name = name.strip()
        if not name or name.startswith("STDERR:") or name.startswith("EXIT_CODE="):
            continue

        dev = f"/dev/{name}"
        result = run_cmd(smartctl_cmd(["-A", dev]), timeout=60)
        if ("sudo: a password is required" in result or "a terminal is required" in result) and SMARTCTL_USE_SUDO:
            result = run_cmd(direct_smartctl_cmd(["-A", dev]), timeout=60)

        disks.append({
            "name": dev,
            "ok": "EXIT_CODE=" not in result and not result.startswith("ERROR"),
            "attributes": parse_smart_text_attributes(result),
            "raw": result,
        })

    return json.dumps({
        "ok": True,
        "disks": disks,
    }, ensure_ascii=False)


@mcp.tool()
def node_storage_usage_json() -> str:
    """Show usage for mounted storage paths that look like Sia or Storj data."""
    return storage_usage_json()


@mcp.tool()
def storage_usage_json() -> str:
    """Show usage for Sia/Storj-like storage mounts across ZFS, btrfs, ext4 and other filesystems."""
    datasets = []
    seen_mounts = set()

    if command_exists(ZFS):
        result = run_cmd([
            ZFS,
            "list",
            "-H",
            "-p",
            "-o",
            "name,used,avail,mountpoint",
        ], timeout=60)

        for line in result.splitlines():
            parts = line.split("\t")
            if len(parts) < 4:
                continue

            name, used, avail, mountpoint = parts[:4]
            if not (name.endswith("/storj") or name.endswith("/sia")):
                continue

            try:
                used_b = int(used)
                avail_b = int(avail)
            except Exception:
                continue

            total_b = used_b + avail_b
            usage_pct = int((used_b * 100) / total_b) if total_b > 0 else 0
            pool = name.split("/", 1)[0]
            node_type = name.rsplit("/", 1)[-1]

            datasets.append({
                "name": name,
                "type": node_type,
                "pool": pool,
                "mountpoint": mountpoint,
                "fstype": "zfs",
                "source": name,
                "used_bytes": used_b,
                "available_bytes": avail_b,
                "total_bytes": total_b,
                "usage_percent": usage_pct,
            })
            seen_mounts.add(mountpoint)

    if not command_exists(FINDMNT):
        return json.dumps({
            "ok": True,
            "datasets": datasets,
            "warning": f"findmnt not found or not executable: {FINDMNT}",
        }, ensure_ascii=False)

    mounts = run_json_cmd([
        FINDMNT,
        "-J",
        "-b",
        "-o",
        "TARGET,SOURCE,FSTYPE,USED,AVAIL,SIZE",
    ], timeout=60)

    for item in mounts.get("data", {}).get("filesystems", []) or []:
        mountpoint = item.get("target") or ""
        low_mount = mountpoint.lower()
        low_source = str(item.get("source") or "").lower()
        low = f"{low_mount} {low_source}"

        if mountpoint in seen_mounts:
            continue
        if "storj" not in low and "sia" not in low:
            continue

        try:
            used_b = int(item.get("used") or 0)
            avail_b = int(item.get("avail") or 0)
            total_b = int(item.get("size") or used_b + avail_b)
        except Exception:
            continue

        usage_pct = int((used_b * 100) / total_b) if total_b > 0 else 0
        node_type = "storj" if "storj" in low else "sia"

        datasets.append({
            "name": mountpoint,
            "type": node_type,
            "pool": "",
            "mountpoint": mountpoint,
            "fstype": item.get("fstype", ""),
            "source": item.get("source", ""),
            "used_bytes": used_b,
            "available_bytes": avail_b,
            "total_bytes": total_b,
            "usage_percent": usage_pct,
        })

    return json.dumps({
        "ok": True,
        "datasets": datasets,
        "findmnt": mounts,
    }, ensure_ascii=False)


@mcp.tool()
def zfs_table_json() -> str:
    """Return all ZFS filesystem and volume rows used by the local zfs-table view."""
    if not command_exists(ZFS):
        return json.dumps({
            "ok": False,
            "error": f"zfs not found or not executable: {ZFS}",
            "datasets": [],
        })

    result = run_cmd([
        ZFS,
        "list",
        "-H",
        "-p",
        "-t",
        "filesystem,volume",
        "-o",
        "name,type,used,avail,mountpoint",
    ], timeout=60)
    datasets = []

    for line in result.splitlines():
        parts = line.split("\t")
        if len(parts) < 5:
            continue

        name, kind, used, available, mountpoint = parts[:5]
        suffix = name.rsplit("/", 1)[-1].lower()
        if "/" not in name:
            display_type = "pool"
        elif suffix in ("storj", "sia"):
            display_type = suffix
        else:
            display_type = kind.lower()

        used_bytes = optional_int(used)
        available_bytes = optional_int(available)
        if used_bytes is None or available_bytes is None:
            continue

        datasets.append({
            "type": display_type,
            "kind": kind.lower(),
            "name": name,
            "pool": name.split("/", 1)[0],
            "parent": name.rsplit("/", 1)[0] if "/" in name else None,
            "depth": name.count("/"),
            "used_bytes": used_bytes,
            "available_bytes": available_bytes,
            "mountpoint": mountpoint,
        })

    return json.dumps({
        "ok": "EXIT_CODE=" not in result and not result.startswith("ERROR"),
        "datasets": datasets,
        "storj_count": sum(1 for item in datasets if item["type"] == "storj"),
        "sia_count": sum(1 for item in datasets if item["type"] == "sia"),
        "pool_count": sum(1 for item in datasets if item["type"] == "pool"),
        "dataset_count": sum(1 for item in datasets if item["type"] != "pool"),
        "filesystem_count": sum(1 for item in datasets if item["type"] == "filesystem"),
        "volume_count": sum(1 for item in datasets if item["kind"] == "volume"),
    }, ensure_ascii=False)


@mcp.tool()
def smart_table_json() -> str:
    """Return the latest smartd ATA/SAS snapshots without querying or waking disks."""
    if not SMART_LOG_DIR.is_dir():
        return json.dumps({
            "ok": False,
            "error": f"SMART log directory does not exist: {SMART_LOG_DIR}",
            "drives": [],
        })

    devices = smartd_device_map()
    paths = sorted({
        *SMART_LOG_DIR.glob("attrlog.*.ata.csv"),
        *SMART_LOG_DIR.glob("attrlog.*.scsi.csv"),
    })
    drives = []
    errors = []

    for path in paths:
        try:
            snapshot = parse_smartd_snapshot(path, devices)
            if snapshot:
                drives.append(snapshot)
        except Exception as exc:
            errors.append(f"{path.name}: {exc}")

    drives.sort(key=lambda item: (not item["connected"], item.get("device") or item["serial"]))

    return json.dumps({
        "ok": bool(drives),
        "source": "smartd_attribute_logs",
        "log_dir": str(SMART_LOG_DIR),
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "drives": drives,
        "connected_count": sum(1 for item in drives if item["connected"]),
        "historical_count": sum(1 for item in drives if not item["connected"]),
        "errors": errors,
    }, ensure_ascii=False)


@mcp.tool()
def smart_nvme_devices_json() -> str:
    """Return live SMART data for NVMe devices without touching spinning disks."""
    if not command_exists(SMARTCTL) or not command_exists(LSBLK):
        return json.dumps({
            "ok": False,
            "error": "smartctl or lsblk is not available.",
            "devices": [],
        })

    block_devices = run_json_cmd([LSBLK, "-J", "-d", "-o", "NAME,TYPE,TRAN"])
    devices = []

    for item in block_devices.get("data", {}).get("blockdevices", []) or []:
        name = str(item.get("name") or "").strip()
        transport = str(item.get("tran") or "").strip().lower()
        if not name or not (name.startswith("nvme") or transport == "nvme"):
            continue

        device = f"/dev/{name}"
        result = run_json_cmd(smartctl_cmd(["-a", "-j", device]), timeout=120)
        if not result.get("ok") and SMARTCTL_USE_SUDO:
            direct_result = run_json_cmd(direct_smartctl_cmd(["-a", "-j", device]), timeout=120)
            result = {
                **direct_result,
                "sudo_error": result,
                "fallback_without_sudo": True,
            }

        devices.append({
            "name": device,
            "type": "nvme",
            "result": result,
        })

    return json.dumps({
        "ok": bool(devices),
        "devices": devices,
    }, ensure_ascii=False)


@mcp.tool()
def smart_devices_json() -> str:
    """Scan SMART-capable devices and return smartctl health/attributes as JSON."""
    if not command_exists(SMARTCTL):
        return json.dumps({
            "ok": False,
            "error": f"smartctl not found or not executable: {SMARTCTL}",
        })

    scan = run_json_cmd(smartctl_cmd(["--scan-open", "-j"]), timeout=60)
    if not scan.get("ok") and SMARTCTL_USE_SUDO:
        fallback_scan = run_json_cmd(direct_smartctl_cmd(["--scan-open", "-j"]), timeout=60)
        scan = {
            **fallback_scan,
            "sudo_error": scan,
            "fallback_without_sudo": True,
        }
    devices = []

    if scan.get("ok"):
        devices = scan.get("data", {}).get("devices", []) or []

    if not devices:
        return json.dumps({
            "ok": False,
            "error": "No SMART devices found by smartctl --scan-open.",
            "scan": scan,
        }, ensure_ascii=False)

    output = {
        "ok": True,
        "devices": [],
        "scan": scan,
    }

    for device in devices:
        name = device.get("name")
        dev_type = device.get("type")

        if not name:
            continue

        args = ["-a", "-j"]
        if dev_type:
            args.extend(["-d", dev_type])
        args.append(name)

        output["devices"].append({
            "name": name,
            "type": dev_type,
            "result": run_json_cmd(smartctl_cmd(args), timeout=120),
        })

        if (
            not output["devices"][-1]["result"].get("ok")
            and SMARTCTL_USE_SUDO
        ):
            direct_result = run_json_cmd(direct_smartctl_cmd(args), timeout=120)
            output["devices"][-1]["result"] = {
                **direct_result,
                "sudo_error": output["devices"][-1]["result"],
                "fallback_without_sudo": True,
            }

    return json.dumps(output, ensure_ascii=False)


@mcp.tool()
def docker_containers_json() -> str:
    """List Docker containers as JSON lines wrapped into one JSON document."""
    if not command_exists(DOCKER):
        return json.dumps({
            "ok": False,
            "error": f"docker not found or not executable: {DOCKER}",
        })

    raw = run_cmd([DOCKER, "ps", "-a", "--no-trunc", "--format", "{{json .}}"], timeout=60)
    containers = []
    errors = []

    for line in raw.splitlines():
        line = line.strip()
        if not line or line.startswith("STDERR:") or line.startswith("EXIT_CODE="):
            continue

        try:
            containers.append(json.loads(line))
        except Exception as e:
            errors.append(f"{e}: {line}")

    return json.dumps({
        "ok": not raw.startswith("ERROR") and "EXIT_CODE=" not in raw,
        "containers": containers,
        "errors": errors,
        "raw": raw,
    }, ensure_ascii=False)


@mcp.tool()
def system_services() -> str:
    """List systemd services so Sia/Storj services can be detected."""
    if not command_exists(SYSTEMCTL):
        return f"systemctl not found or not executable: {SYSTEMCTL}"

    return run_cmd([
        SYSTEMCTL,
        "list-units",
        "--type=service",
        "--all",
        "--no-pager",
        "--plain",
    ], timeout=60)


@mcp.tool()
def zpool_list() -> str:
    """Show ZFS pools with zpool list."""
    return run_cmd([ZPOOL, "list"])


@mcp.tool()
def zpool_status() -> str:
    """Show full ZFS pool status with zpool status."""
    return run_cmd([ZPOOL, "status"], timeout=120)


@mcp.tool()
def zpool_status_x() -> str:
    """Show ZFS pool health summary with zpool status -x."""
    return run_cmd([ZPOOL, "status", "-x"])


@mcp.tool()
def zfs_list() -> str:
    """Show ZFS datasets with usage, quota and mountpoints."""
    return run_cmd([
        ZFS,
        "list",
        "-o",
        "name,used,avail,refer,quota,refquota,mountpoint",
        "-t",
        "filesystem,volume",
    ])


@mcp.tool()
def list_allowed_dir(path: str) -> str:
    """List files in an allowed directory. Only paths from FS_ALLOWED_ROOTS are allowed."""
    target = ensure_allowed_path(path)

    if not target.exists():
        return f"Path does not exist: {target}"

    if not target.is_dir():
        return f"Path is not a directory: {target}"

    lines = [f"Directory: {target}", ""]

    for item in sorted(target.iterdir(), key=lambda x: (not x.is_dir(), x.name.lower())):
        kind = "DIR " if item.is_dir() else "FILE"
        lines.append(f"{kind} {item}")

    return "\n".join(lines)


@mcp.tool()
def read_text_file(path: str) -> str:
    """Read a text file from allowed roots. Size is limited by MAX_READ_FILE_BYTES."""
    target = ensure_allowed_path(path)

    if not target.exists():
        return f"File does not exist: {target}"

    if not target.is_file():
        return f"Path is not a file: {target}"

    size = target.stat().st_size
    if size > MAX_READ_FILE_BYTES:
        return (
            f"File is too large: {size} bytes. "
            f"Limit: {MAX_READ_FILE_BYTES} bytes."
        )

    try:
        return target.read_text(errors="replace")
    except Exception as e:
        return f"Cannot read file: {e}"


if __name__ == "__main__":
    mcp.run(
        transport="http",
        host=MCP_HOST,
        port=MCP_PORT,
    )
