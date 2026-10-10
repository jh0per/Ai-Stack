#!/usr/bin/env python3
import importlib.util
import json
import os
import signal
import sys
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[1]
SERVER_PATH = ROOT_DIR / "mcp-storage-server" / "server.py"
HEADERS = ["DATASET", "USED", "FREE", "USAGE", "MOUNTPOINT"]
WIDTH_CAPS = [42, 8, 8, 17, 42]
RIGHT_ALIGNED = {1, 2}
signal.signal(signal.SIGPIPE, signal.SIG_DFL)
USE_COLOR = sys.stdout.isatty() and not os.getenv("NO_COLOR")
RESET = "\033[0m" if USE_COLOR else ""
BOLD = "\033[1m" if USE_COLOR else ""
DIM = "\033[2m" if USE_COLOR else ""
CYAN = "\033[38;5;45m" if USE_COLOR else ""
MAGENTA = "\033[38;5;213m" if USE_COLOR else ""
BLUE = "\033[38;5;75m" if USE_COLOR else ""
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


def usage_percent(used, available) -> float:
    total = (used or 0) + (available or 0)
    return (used or 0) * 100 / total if total > 0 else 0.0


def progress_text(percent: float, width: int = 12) -> str:
    used_cells = min(width, max(0, round(percent * width / 100)))
    return f"{'█' * used_cells}{'░' * (width - used_cells)} {percent:3.0f}%"


def colored_progress(percent: float, width: int = 12) -> str:
    used_cells = min(width, max(0, round(percent * width / 100)))
    used_color = RED if percent >= 90 else YELLOW
    return (
        f"{used_color}{'█' * used_cells}{RESET}"
        f"{GREEN}{'░' * (width - used_cells)}{RESET} "
        f"{used_color}{percent:3.0f}%{RESET}"
    )


def tree_rows(datasets: list[dict]) -> list[tuple[dict, str]]:
    by_name = {item["name"]: item for item in datasets}
    children = {name: [] for name in by_name}
    roots = []

    for item in datasets:
        parent = item.get("parent")
        if parent in by_name:
            children[parent].append(item["name"])
        else:
            roots.append(item["name"])

    output = []

    def visit(name: str, prefix: str, connector: str) -> None:
        label = name if not connector else f"{prefix}{connector}{name.rsplit('/', 1)[-1]}"
        output.append((by_name[name], label))
        names = children[name]
        child_prefix = prefix + ("    " if connector == "└── " else "│   " if connector else "")
        for index, child_name in enumerate(names):
            child_connector = "└── " if index == len(names) - 1 else "├── "
            visit(child_name, child_prefix, child_connector)

    for root in roots:
        visit(root, "", "")

    return output


def node_color(item: dict) -> str:
    if item.get("is_root"):
        return GREEN
    if item.get("tag") == "storj":
        return CYAN
    if item.get("tag") == "sia":
        return MAGENTA
    if item.get("type") == "volume":
        return YELLOW
    return BLUE


def colored_tree(value: str, width: int, item: dict) -> str:
    padded = value.ljust(width)
    color = node_color(item)
    if item.get("is_root"):
        return f"{BOLD}{color}{padded}{RESET}"

    positions = [value.rfind("├── "), value.rfind("└── ")]
    split_at = max(positions)
    if split_at < 0:
        return f"{color}{padded}{RESET}"

    name_at = split_at + 4
    prefix = value[:name_at]
    name = value[name_at:]
    padding = " " * max(0, width - len(value))
    return f"{DIM}{prefix}{RESET}{color}{name}{RESET}{padding}"


def render_table(rows: list[list[str]], percentages: list[float], items: list[dict]) -> str:
    widths = []
    for index, header in enumerate(HEADERS):
        widest = max([len(header), *(len(row[index]) for row in rows)])
        widths.append(min(widest, WIDTH_CAPS[index]))

    def border(left: str, middle: str, right: str) -> str:
        line = left + middle.join("─" * (width + 2) for width in widths) + right
        return f"{DIM}{line}{RESET}"

    def table_row(values: list[str], item: dict | None = None, percent: float | None = None) -> str:
        output = []
        for index, value in enumerate(values):
            value = fit(value, widths[index])
            value = value.rjust(widths[index]) if index in RIGHT_ALIGNED else value.ljust(widths[index])
            color = ""
            if index == 1 and percent is not None:
                color = RED if percent >= 90 else YELLOW
            elif index == 2 and percent is not None:
                color = GREEN

            if index == 0 and item is not None:
                value = colored_tree(value.rstrip(), widths[index], item)
            elif index == 3 and percent is not None:
                value = colored_progress(percent) if USE_COLOR else progress_text(percent)
                value = value.ljust(widths[index])

            output.append(f" {color}{value}{RESET if color else ''} ")
        return f"{DIM}│{RESET}" + f"{DIM}│{RESET}".join(output) + f"{DIM}│{RESET}"

    return "\n".join([
        border("╭", "┬", "╮"),
        f"{BOLD}{table_row(HEADERS)}{RESET}",
        border("├", "┼", "┤"),
        *(table_row(row, item, percent) for row, percent, item in zip(rows, percentages, items)),
        border("╰", "┴", "╯"),
    ])


def render() -> str:
    server = load_storage_server()
    data = json.loads(server.zfs_table_json())
    datasets = data.get("datasets", [])
    if not datasets:
        raise RuntimeError(data.get("error") or "No ZFS filesystems or volumes were found.")

    ordered = tree_rows(datasets)
    percentages = [
        usage_percent(item.get("used_bytes"), item.get("available_bytes"))
        for item, _ in ordered
    ]
    rows = [
        [
            label,
            human_bytes(item.get("used_bytes")),
            human_bytes(item.get("available_bytes")),
            progress_text(percent),
            cell(item.get("mountpoint")),
        ]
        for (item, label), percent in zip(ordered, percentages)
    ]
    colored_summary = (
        f"{GREEN}●{RESET} {len(datasets)} entries · "
        f"{GREEN}ROOTS{RESET} {data.get('root_count', 0)} · "
        f"{BLUE}CHILDREN{RESET} {data.get('dataset_count', 0)} · "
        f"{CYAN}STORJ{RESET} {data.get('storj_count', 0)} · "
        f"{MAGENTA}SIA{RESET} {data.get('sia_count', 0)}"
    )
    return "\n".join([
        f"{BOLD}{CYAN}ZFS Storage Datasets{RESET}  {DIM}all filesystems and volumes{RESET}",
        render_table(rows, percentages, [item for item, _ in ordered]),
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
