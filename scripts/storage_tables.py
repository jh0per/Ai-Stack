#!/usr/bin/env python3
import argparse
import importlib.util
import json
import sys
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[1]
SERVER_PATH = ROOT_DIR / "mcp-storage-server" / "server.py"


def load_storage_server():
    spec = importlib.util.spec_from_file_location("ai_stack_storage_server", SERVER_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Cannot load {SERVER_PATH}")

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def cell_text(value) -> str:
    return "-" if value is None or value == "" else str(value)


def truncate(value: str, width: int) -> str:
    if len(value) <= width:
        return value
    if width <= 1:
        return value[:width]
    return value[: width - 1] + "…"


def render_table(headers: list[str], rows: list[list[str]], caps: list[int], right: set[int]) -> str:
    widths = []
    for index, header in enumerate(headers):
        content_width = max([len(header), *(len(row[index]) for row in rows)], default=len(header))
        widths.append(min(content_width, caps[index]))

    def border(left: str, middle: str, right_edge: str) -> str:
        return left + middle.join("─" * (width + 2) for width in widths) + right_edge

    def row_line(values: list[str]) -> str:
        cells = []
        for index, value in enumerate(values):
            value = truncate(value, widths[index])
            aligned = value.rjust(widths[index]) if index in right else value.ljust(widths[index])
            cells.append(f" {aligned} ")
        return "│" + "│".join(cells) + "│"

    output = [
        border("╭", "┬", "╮"),
        row_line(headers),
        border("├", "┼", "┤"),
    ]
    output.extend(row_line(row) for row in rows)
    output.append(border("╰", "┴", "╯"))
    return "\n".join(output)


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


def smart_table(server) -> str:
    data = json.loads(server.smart_table_json())
    drives = data.get("drives", [])
    if not drives:
        raise RuntimeError(data.get("error") or "No smartd attribute snapshots found.")

    rows = []
    for drive in drives:
        label = " ".join(part for part in (drive.get("model"), drive.get("serial")) if part)
        rows.append([
            label or "-",
            drive.get("device") or "?",
            cell_text(drive.get("temperature_c")),
            cell_text(drive.get("reallocated")),
            cell_text(drive.get("pending")),
            cell_text(drive.get("uncorrectable")),
            cell_text(drive.get("power_on_hours")),
            cell_text(drive.get("snapshot_at")),
        ])

    connected = [drive for drive in drives if drive.get("connected")]
    historical = [drive for drive in drives if not drive.get("connected")]
    active_warnings = sum(1 for drive in connected if int(drive.get("severity") or 0) >= 2)
    historical_warnings = sum(1 for drive in historical if int(drive.get("severity") or 0) >= 2)
    summary = (
        f"{len(connected)} connected · {len(historical)} historical · "
        f"{active_warnings} active warnings · {historical_warnings} historical warnings"
    )

    return "\n".join([
        "SMART Disk Health  smartd snapshots",
        render_table(
            ["DISK", "DEV", "TEMP", "RELO", "PEND", "UNCOR", "HOURS", "SNAPSHOT"],
            rows,
            [48, 12, 4, 5, 5, 5, 8, 19],
            {2, 3, 4, 5, 6},
        ),
        summary,
    ])


def zfs_table(server) -> str:
    data = json.loads(server.zfs_table_json())
    datasets = data.get("datasets", [])
    if not datasets:
        raise RuntimeError(data.get("error") or "No ZFS datasets ending in /storj or /sia were found.")

    rows = [
        [
            cell_text(item.get("type")).upper(),
            cell_text(item.get("name")),
            human_bytes(item.get("used_bytes")),
            human_bytes(item.get("available_bytes")),
            cell_text(item.get("mountpoint")),
        ]
        for item in datasets
    ]
    summary = (
        f"{len(datasets)} datasets · STORJ {data.get('storj_count', 0)} · "
        f"SIA {data.get('sia_count', 0)}"
    )

    return "\n".join([
        "ZFS Storage Datasets  Storj and Sia",
        render_table(
            ["TYPE", "DATASET", "USED", "FREE", "MOUNTPOINT"],
            rows,
            [5, 42, 8, 8, 42],
            {2, 3},
        ),
        summary,
    ])


def main(table: str | None = None) -> int:
    if table is None:
        parser = argparse.ArgumentParser(description="Render local storage tables.")
        parser.add_argument("table", choices=("smart", "zfs"))
        table = parser.parse_args().table

    try:
        server = load_storage_server()
        print(smart_table(server) if table == "smart" else zfs_table(server))
        return 0
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
