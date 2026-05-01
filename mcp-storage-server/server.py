import os
import json
import subprocess
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
