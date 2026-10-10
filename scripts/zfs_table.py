#!/usr/bin/env python3
import importlib.util
import json
import os
import sys
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[1]
SERVER_PATH = ROOT_DIR / "mcp-storage-server" / "server.py"
HEADERS = ["TYPE", "DATASET", "USED", "FREE", "MOUNTPOINT"]
WIDTH_CAPS = [5, 42, 8, 8, 42]
RIGHT_ALIGNED = {2, 3}
USE_COLOR = sys.stdout.isatty() and not os.getenv("NO_COLOR")
RESET = "\033[0m" if USE_COLOR else ""
BOLD = "\033[1m" if USE_COLOR else ""
DIM = "\033[2m" if USE_COLOR else ""
CYAN = "\033[38;5;45m" if USE_COLOR else ""
MAGENTA = "\033[38;5;213m" if USE_COLOR else ""
GREEN = "\033[38;5;42m" if USE_COLOR else ""


def load_storage_server():
    spec = importlib.util.spec_from_file_location("ai_stack_storage_server", SERVER_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Cannot load {SERVER_PATH}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def cell(value) -> str:
    return "-" if value is None or value == "" else str(value)


def human_bytes(value) -> str:
    if value is None:
        return "-"
    number = float(value)
    units = ("B", "K", "M", "G", "T", "P")
    unit = 0
    while number >= 1024 and unit < len(units) - 1:
        number /= 1024
        unit += 1
    return f"{number:.0f}{units[unit]}" if unit <= 2 else f"{number:.2f}{units[unit]}"


def fit(value: str, width: int) -> str:
    if len(value) <= width:
        return value
    return value[: width - 1] + "…"


def render_table(rows: list[list[str]]) -> str:
    widths = []
    for index, header in enumerate(HEADERS):
        widest = max([len(header), *(len(row[index]) for row in rows)])
        widths.append(min(widest, WIDTH_CAPS[index]))

    def border(left: str, middle: str, right: str) -> str:
        line = left + middle.join("─" * (width + 2) for width in widths) + right
        return f"{DIM}{line}{RESET}"

    def table_row(values: list[str], row_type: str = "") -> str:
        output = []
        for index, value in enumerate(values):
            value = fit(value, widths[index])
            value = value.rjust(widths[index]) if index in RIGHT_ALIGNED else value.ljust(widths[index])
            color = CYAN if index == 0 and row_type == "storj" else MAGENTA if index == 0 and row_type == "sia" else ""
            output.append(f" {color}{value}{RESET if color else ''} ")
        return f"{DIM}│{RESET}" + f"{DIM}│{RESET}".join(output) + f"{DIM}│{RESET}"

    return "\n".join([
        border("╭", "┬", "╮"),
        f"{BOLD}{table_row(HEADERS)}{RESET}",
        border("├", "┼", "┤"),
        *(table_row(row, row[0].lower()) for row in rows),
        border("╰", "┴", "╯"),
    ])


def render() -> str:
    server = load_storage_server()
    data = json.loads(server.zfs_table_json())
    datasets = data.get("datasets", [])
    if not datasets:
        raise RuntimeError(data.get("error") or "No ZFS datasets ending in /storj or /sia were found.")

    rows = [
        [
            cell(item.get("type")).upper(),
            cell(item.get("name")),
            human_bytes(item.get("used_bytes")),
            human_bytes(item.get("available_bytes")),
            cell(item.get("mountpoint")),
        ]
        for item in datasets
    ]
    colored_summary = (
        f"{GREEN}●{RESET} {len(datasets)} datasets · "
        f"{CYAN}STORJ{RESET} {data.get('storj_count', 0)} · "
        f"{MAGENTA}SIA{RESET} {data.get('sia_count', 0)}"
    )
    return "\n".join([
        f"{BOLD}{CYAN}ZFS Storage Datasets{RESET}  {DIM}Storj and Sia{RESET}",
        render_table(rows),
        colored_summary,
    ])


def main() -> int:
    try:
        print(render())
        return 0
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
