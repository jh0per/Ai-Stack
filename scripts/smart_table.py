#!/usr/bin/env python3
import importlib.util
import json
import os
import signal
import sys
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[1]
SERVER_PATH = ROOT_DIR / "mcp-storage-server" / "server.py"
HEADERS = ["DISK", "DEV", "TEMP", "RELO", "PEND", "UNCOR", "HOURS", "SNAPSHOT"]
WIDTHS = [48, 12, 4, 5, 5, 5, 8, 19]
RIGHT_ALIGNED = {2, 3, 4, 5, 6}
signal.signal(signal.SIGPIPE, signal.SIG_DFL)
USE_COLOR = sys.stdout.isatty() and not os.getenv("NO_COLOR")
RESET = "\033[0m" if USE_COLOR else ""
BOLD = "\033[1m" if USE_COLOR else ""
DIM = "\033[2m" if USE_COLOR else ""
CYAN = "\033[38;5;45m" if USE_COLOR else ""
GREEN = "\033[38;5;42m" if USE_COLOR else ""
YELLOW = "\033[38;5;220m" if USE_COLOR else ""
RED = "\033[38;5;196m" if USE_COLOR else ""


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
    line = left + middle.join("─" * (width + 2) for width in WIDTHS) + right
    return f"{DIM}{line}{RESET}"


def metric_color(value: str, temperature: bool = False) -> str:
    try:
        number = int(value)
    except ValueError:
        return DIM

    if temperature:
        if number > 51:
            return RED
        if number > 41:
            return YELLOW
        return GREEN

    if number > 5:
        return RED
    if number >= 1:
        return YELLOW
    return GREEN


def table_row(values: list[str], drive: dict | None = None) -> str:
    output = []
    for index, value in enumerate(values):
        value = fit(value, WIDTHS[index])
        value = value.rjust(WIDTHS[index]) if index in RIGHT_ALIGNED else value.ljust(WIDTHS[index])
        color = ""
        if drive is not None:
            if index == 2:
                color = metric_color(cell(drive.get("temperature_c")), temperature=True)
            elif index in (3, 4, 5):
                keys = {3: "reallocated", 4: "pending", 5: "uncorrectable"}
                color = metric_color(cell(drive.get(keys[index])))
            elif not drive.get("connected"):
                color = DIM
        output.append(f" {color}{value}{RESET if color else ''} ")
    return f"{DIM}│{RESET}" + f"{DIM}│{RESET}".join(output) + f"{DIM}│{RESET}"


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
        f"{BOLD}{CYAN}SMART Disk Health{RESET}  {DIM}smartd snapshots{RESET}",
        border("╭", "┬", "╮"),
        f"{BOLD}{table_row(HEADERS)}{RESET}",
        border("├", "┼", "┤"),
        *(table_row(row, drive) for row, drive in zip(rows, drives)),
        border("╰", "┴", "╯"),
        (
            f"{GREEN}●{RESET} {len(connected)} connected · "
            f"{DIM}○ {len(historical)} historical{RESET} · "
            f"{RED if active_warnings else GREEN}{'▲' if active_warnings else '✓'}{RESET} "
            f"{active_warnings} active warnings · "
            f"{YELLOW if historical_warnings else DIM}{historical_warnings} historical warnings{RESET}"
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
