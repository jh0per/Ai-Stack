#!/usr/bin/env python3
import importlib.util
import json
import sys
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[1]
SERVER_PATH = ROOT_DIR / "mcp-storage-server" / "server.py"
HEADERS = ["TYPE", "DATASET", "USED", "FREE", "MOUNTPOINT"]
WIDTH_CAPS = [5, 42, 8, 8, 42]
RIGHT_ALIGNED = {2, 3}


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
        return left + middle.join("─" * (width + 2) for width in widths) + right

    def table_row(values: list[str]) -> str:
        output = []
        for index, value in enumerate(values):
            value = fit(value, widths[index])
            value = value.rjust(widths[index]) if index in RIGHT_ALIGNED else value.ljust(widths[index])
            output.append(f" {value} ")
        return "│" + "│".join(output) + "│"

    return "\n".join([
        border("╭", "┬", "╮"),
        table_row(HEADERS),
        border("├", "┼", "┤"),
        *(table_row(row) for row in rows),
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
    summary = (
        f"{len(datasets)} datasets · STORJ {data.get('storj_count', 0)} · "
        f"SIA {data.get('sia_count', 0)}"
    )
    return "\n".join(["ZFS Storage Datasets  Storj and Sia", render_table(rows), summary])


def main() -> int:
    try:
        print(render())
        return 0
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
