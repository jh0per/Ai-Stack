#!/usr/bin/env python3
import importlib.util
import json
import sys
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[1]
SERVER_PATH = ROOT_DIR / "mcp-storage-server" / "server.py"
HEADERS = ["DISK", "DEV", "TEMP", "RELO", "PEND", "UNCOR", "HOURS", "SNAPSHOT"]
WIDTHS = [48, 12, 4, 5, 5, 5, 8, 19]
RIGHT_ALIGNED = {2, 3, 4, 5, 6}


def load_storage_server():
    spec = importlib.util.spec_from_file_location("ai_stack_storage_server", SERVER_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Cannot load {SERVER_PATH}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def cell(value) -> str:
    return "-" if value is None or value == "" else str(value)


def fit(value: str, width: int) -> str:
    if len(value) <= width:
        return value
    return value[: width - 1] + "…"


def border(left: str, middle: str, right: str) -> str:
    return left + middle.join("─" * (width + 2) for width in WIDTHS) + right


def table_row(values: list[str]) -> str:
    output = []
    for index, value in enumerate(values):
        value = fit(value, WIDTHS[index])
        value = value.rjust(WIDTHS[index]) if index in RIGHT_ALIGNED else value.ljust(WIDTHS[index])
        output.append(f" {value} ")
    return "│" + "│".join(output) + "│"


def render() -> str:
    server = load_storage_server()
    data = json.loads(server.smart_table_json())
    drives = data.get("drives", [])
    if not drives:
        raise RuntimeError(data.get("error") or "No smartd attribute snapshots found.")

    rows = []
    for drive in drives:
        disk = " ".join(part for part in (drive.get("model"), drive.get("serial")) if part)
        rows.append([
            disk or "-",
            drive.get("device") or "?",
            cell(drive.get("temperature_c")),
            cell(drive.get("reallocated")),
            cell(drive.get("pending")),
            cell(drive.get("uncorrectable")),
            cell(drive.get("power_on_hours")),
            cell(drive.get("snapshot_at")),
        ])

    connected = [drive for drive in drives if drive.get("connected")]
    historical = [drive for drive in drives if not drive.get("connected")]
    active_warnings = sum(1 for drive in connected if int(drive.get("severity") or 0) >= 2)
    historical_warnings = sum(1 for drive in historical if int(drive.get("severity") or 0) >= 2)

    lines = [
        "SMART Disk Health  smartd snapshots",
        border("╭", "┬", "╮"),
        table_row(HEADERS),
        border("├", "┼", "┤"),
        *(table_row(row) for row in rows),
        border("╰", "┴", "╯"),
        (
            f"{len(connected)} connected · {len(historical)} historical · "
            f"{active_warnings} active warnings · {historical_warnings} historical warnings"
        ),
    ]
    return "\n".join(lines)


def main() -> int:
    try:
        print(render())
        return 0
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
