#!/usr/bin/env python3
"""Render live Storj and Sia container inventories as terminal tables."""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import signal
import socket
import subprocess
import sys
import textwrap
from pathlib import Path
from typing import Any


signal.signal(signal.SIGPIPE, signal.SIG_DFL)

USE_COLOR = sys.stdout.isatty() and not os.getenv("NO_COLOR")
RESET = "\033[0m" if USE_COLOR else ""
BOLD = "\033[1m" if USE_COLOR else ""
DIM = "\033[2m" if USE_COLOR else ""
CYAN = "\033[38;5;45m" if USE_COLOR else ""
MAGENTA = "\033[38;5;213m" if USE_COLOR else ""
GREEN = "\033[38;5;42m" if USE_COLOR else ""
YELLOW = "\033[38;5;220m" if USE_COLOR else ""
RED = "\033[38;5;196m" if USE_COLOR else ""
CONTROL_CHARACTERS = re.compile(r"[\x00-\x08\x0b-\x1f\x7f-\x9f]")

STORJ_HEADERS = [
    "SERVER",
    "NODE",
    "MOUNTS",
    "PORTS",
    "ADDRESS",
    "DOCKER IP",
    "RUN SCRIPT",
]
STORJ_CAPS = [12, 19, 42, 18, 26, 18, 38]
STORJ_MINS = [5, 9, 12, 8, 10, 9, 12]
STORJ_GROUPED_HEADERS = ["NODE", "STORAGE", "NETWORK", "RUNTIME"]
STORJ_GROUPED_CAPS = [20, 48, 42, 46]
STORJ_GROUPED_MINS = [10, 20, 20, 16]
SIA_HEADERS = [
    "SERVER",
    "NODE",
    "MOUNTS",
    "PORTS",
    "LISTEN",
    "DOCKER IP",
    "COMPOSE",
    "DATA PATH",
]
SIA_CAPS = [12, 20, 52, 18, 42, 18, 42, 38]
SIA_MINS = [5, 9, 12, 8, 12, 9, 12, 12]
SIA_GROUPED_HEADERS = ["NODE", "STORAGE", "NETWORK", "RUNTIME"]
SIA_GROUPED_CAPS = [20, 50, 42, 46]
SIA_GROUPED_MINS = [10, 20, 20, 16]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Show live Storj and Sia container inventories."
    )
    parser.add_argument(
        "path",
        type=Path,
        help="directory containing Storj run and/or Sia Compose directories",
    )
    parser.add_argument(
        "--only",
        choices=("all", "storj", "sia"),
        default="all",
        help="render one table instead of both",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="print structured, untruncated JSON instead of terminal tables",
    )
    parser.add_argument(
        "--wide",
        action="store_true",
        help="render every row on one unconstrained line",
    )
    parser.add_argument(
        "--width",
        type=int,
        help="override detected terminal width",
    )
    return parser.parse_args()


def docker_inventory() -> list[dict[str, Any]]:
    docker = shutil.which("docker")
    if not docker:
        raise RuntimeError("docker command was not found")

    listed = subprocess.run(
        [docker, "ps", "-aq"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.split()
    if not listed:
        return []

    inspected = subprocess.run(
        [docker, "inspect", *listed],
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    return json.loads(inspected)


def clean_name(container: dict[str, Any]) -> str:
    return str(container.get("Name") or "").lstrip("/")


def node_number(name: str) -> int | None:
    match = re.search(r"(?:_|-)(\d{4,5})(?:\D|$)", name)
    return int(match.group(1)) if match else None


def natural_key(value: str) -> tuple[int, str]:
    number = node_number(value)
    return (number if number is not None else sys.maxsize, value)


def environment(container: dict[str, Any]) -> dict[str, str]:
    output: dict[str, str] = {}
    for item in container.get("Config", {}).get("Env") or []:
        key, separator, value = str(item).partition("=")
        if separator:
            output[key] = value
    return output


def is_running(container: dict[str, Any]) -> bool:
    return bool(container.get("State", {}).get("Running"))


def container_ips(container: dict[str, Any]) -> list[str]:
    networks = container.get("NetworkSettings", {}).get("Networks") or {}
    return sorted(
        {
            str(network.get("IPAddress"))
            for network in networks.values()
            if network.get("IPAddress")
        }
    )


def host_ports(container: dict[str, Any]) -> list[int]:
    bindings = container.get("HostConfig", {}).get("PortBindings") or {}
    ports: set[int] = set()
    for entries in bindings.values():
        for entry in entries or []:
            value = str(entry.get("HostPort") or "")
            if value.isdigit():
                ports.add(int(value))
    return sorted(ports)


def configured_listeners(container: dict[str, Any]) -> list[str]:
    listeners = set()
    for argument in container.get("Config", {}).get("Cmd") or []:
        value = str(argument)
        match = re.fullmatch(r"(?:[^:]+)?:([0-9]{2,5})", value)
        if match:
            listeners.add(value)
    return sorted(listeners, key=lambda value: int(value.rsplit(":", 1)[-1]))


def configured_ports(container: dict[str, Any]) -> list[int]:
    ports = set(host_ports(container))
    for listener in configured_listeners(container):
        ports.add(int(listener.rsplit(":", 1)[-1]))
    return sorted(ports)


def compact_ports(ports: list[int]) -> str:
    if not ports:
        return "-"
    if ports == list(range(ports[0], ports[-1] + 1)) and len(ports) > 2:
        return f"{ports[0]}-{ports[-1]}"
    return ", ".join(str(port) for port in ports)


def mounts(container: dict[str, Any]) -> list[dict[str, str]]:
    return [
        {
            "source": str(item.get("Source") or ""),
            "destination": str(item.get("Destination") or ""),
        }
        for item in container.get("Mounts") or []
        if item.get("Source")
    ]


def text(value: Any) -> str:
    if value is None or value == "":
        return "-"
    if isinstance(value, list):
        return "; ".join(text(item) for item in value) or "-"
    if isinstance(value, dict):
        return json.dumps(value, ensure_ascii=False, separators=(",", ":"))
    cleaned = str(value).replace("\r", " ").replace("\n", "; ")
    return CONTROL_CHARACTERS.sub("?", cleaned)


def storj_run_script(container_mounts: list[dict[str, str]]) -> str:
    for item in container_mounts:
        if item["destination"] == "/app/identity":
            source = Path(item["source"])
            directory = source.parent if source.name == "identity" else source
            for filename in ("run.sh", "run"):
                candidate = directory / filename
                if candidate.is_file():
                    return str(candidate)
            return str(directory)
    return "-"


def sia_compose_file(container: dict[str, Any]) -> str:
    labels = container.get("Config", {}).get("Labels") or {}
    config_files = labels.get("com.docker.compose.project.config_files")
    if config_files:
        return str(config_files).split(",", 1)[0]
    working_dir = labels.get("com.docker.compose.project.working_dir")
    if working_dir:
        directory = Path(str(working_dir))
        for filename in ("compose.yml", "compose.yaml", "docker-compose.yml", "docker-compose.yaml"):
            candidate = directory / filename
            if candidate.is_file():
                return str(candidate)
        return str(directory)
    return "-"


def path_is_within(candidate: str, root: Path) -> bool:
    if not candidate or candidate == "-":
        return False
    try:
        Path(candidate).resolve(strict=False).relative_to(root)
        return True
    except ValueError:
        return False


def classify(container: dict[str, Any]) -> str | None:
    name = clean_name(container).lower()
    image = str(container.get("Config", {}).get("Image") or "").lower()
    if "storagenode" in name or "storjlabs/storagenode" in image:
        return "storj"
    if re.search(r"(?:^|[_-])sia[_-]", name) or "siafoundation/hostd" in image:
        return "sia"
    return None


def storj_rows(
    containers: list[dict[str, Any]], root: Path, hostname: str
) -> list[dict[str, Any]]:
    output = []
    for container in containers:
        if classify(container) != "storj":
            continue
        name = clean_name(container)
        mounted = mounts(container)
        storage_mounts = [
            item["source"]
            for item in mounted
            if item["destination"] == "/app/config"
        ]
        run_script = storj_run_script(mounted)
        related_paths = [run_script, *(item["source"] for item in mounted)]
        if not any(path_is_within(path, root) for path in related_paths):
            continue
        env = environment(container)
        ports = configured_ports(container)
        output.append(
            {
                "server": hostname,
                "node": name,
                "mounts": storage_mounts,
                "ports": ports,
                "address": env.get("ADDRESS"),
                "docker_ip": container_ips(container),
                "run_script": run_script,
                "running": is_running(container),
            }
        )
    return sorted(output, key=lambda item: natural_key(item["node"]))


def sia_rows(
    containers: list[dict[str, Any]], root: Path, hostname: str
) -> list[dict[str, Any]]:
    output = []
    for container in containers:
        if classify(container) != "sia":
            continue
        name = clean_name(container)
        mounted = mounts(container)
        storage_mounts = [
            item["source"] for item in mounted if item["destination"] != "/data"
        ]
        data_mount = next(
            (
                item["source"]
                for item in mounted
                if item["destination"] == "/data"
            ),
            "-",
        )
        compose_file = sia_compose_file(container)
        related_paths = [compose_file, data_mount, *(item["source"] for item in mounted)]
        if not any(path_is_within(path, root) for path in related_paths):
            continue
        ports = configured_ports(container)
        output.append(
            {
                "server": hostname,
                "node": name,
                "mounts": storage_mounts,
                "ports": ports,
                "listeners": configured_listeners(container),
                "docker_ip": container_ips(container),
                "compose_file": compose_file,
                "data_path": data_mount,
                "running": is_running(container),
            }
        )
    return sorted(output, key=lambda item: natural_key(item["node"]))


def fitted_widths(
    headers: list[str],
    rows: list[list[str]],
    caps: list[int],
    minimums: list[int],
    terminal_width: int,
    wide: bool,
) -> list[int]:
    desired = []
    for index, header in enumerate(headers):
        widest = max(
            [len(header), *(len(row[index]) for row in rows)],
            default=len(header),
        )
        desired.append(widest if wide else min(widest, caps[index]))
    if wide:
        return desired

    # A box table uses three framing characters per column plus one final edge.
    content_budget = max(len(headers), terminal_width - (3 * len(headers) + 1))
    widths = [min(value, 3) for value in desired]

    def grow(targets: list[int]) -> None:
        nonlocal content_budget
        while sum(widths) < content_budget:
            changed = False
            for index, target in enumerate(targets):
                if widths[index] >= target:
                    continue
                widths[index] += 1
                changed = True
                if sum(widths) >= content_budget:
                    return
            if not changed:
                return

    grow([min(value, minimums[index]) for index, value in enumerate(desired)])
    grow(desired)
    return widths


def wrapped_cell(value: str, width: int, wide: bool) -> list[str]:
    if wide or len(value) <= width:
        return value.splitlines() or [""]
    output = []
    for paragraph in value.splitlines() or [""]:
        output.extend(
            textwrap.wrap(
                paragraph,
                width=width,
                break_long_words=True,
                break_on_hyphens=False,
                replace_whitespace=False,
                drop_whitespace=True,
            ) or [""]
        )
    return output


def render_table(
    title: str,
    headers: list[str],
    caps: list[int],
    minimums: list[int],
    rows: list[list[str]],
    running: list[bool],
    accent: str,
    wide: bool,
    terminal_width: int,
    node_column: int,
) -> str:
    widths = fitted_widths(
        headers, rows, caps, minimums, terminal_width, wide
    )

    def border(left: str, middle: str, right: str) -> str:
        value = left + middle.join("─" * (width + 2) for width in widths) + right
        return f"{DIM}{value}{RESET}"

    def table_row(values: list[str], row_running: bool | None = None) -> list[str]:
        wrapped = [
            wrapped_cell(value, widths[index], wide)
            for index, value in enumerate(values)
        ]
        height = max(len(lines) for lines in wrapped)
        output = []
        for line_number in range(height):
            cells = []
            for index, lines in enumerate(wrapped):
                value = lines[line_number] if line_number < len(lines) else ""
                fitted = value.ljust(widths[index])
                color = ""
                if row_running is not None and index == node_column:
                    color = GREEN if row_running else RED
                cells.append(f" {color}{fitted}{RESET if color else ''} ")
            output.append(
                f"{DIM}│{RESET}"
                + f"{DIM}│{RESET}".join(cells)
                + f"{DIM}│{RESET}"
            )
        return output

    body = [
        f"{BOLD}{accent}{title}{RESET}  {DIM}{len(rows)} nodes{RESET}",
        border("╭", "┬", "╮"),
    ]
    body.extend(f"{BOLD}{line}{RESET}" for line in table_row(headers))
    body.append(border("├", "┼", "┤"))
    for index, (row, state) in enumerate(zip(rows, running)):
        if index:
            body.append(border("├", "┼", "┤"))
        body.extend(table_row(row, state))
    body.append(border("╰", "┴", "╯"))
    return "\n".join(body)


def storj_table(
    rows: list[dict[str, Any]], wide: bool, terminal_width: int
) -> str:
    full_display = [
        [
            text(row["server"]),
            text(row["node"]),
            text(row["mounts"]),
            ", ".join(
                str(port)
                for port in sorted(
                    row["ports"],
                    key=lambda port: (port != node_number(row["node"]), port),
                )
            ) or "-",
            text(row["address"]),
            text(row["docker_ip"]),
            text(row["run_script"]),
        ]
        for row in rows
    ]
    if not wide:
        display = [
            [
                text(row["node"]),
                f"Server: {text(row['server'])}\nMounts: {text(row['mounts'])}",
                (
                    f"Ports: {full_display[index][3]}\n"
                    f"Address: {text(row['address'])}\n"
                    f"Docker: {text(row['docker_ip'])}"
                ),
                f"Run script: {text(row['run_script'])}",
            ]
            for index, row in enumerate(rows)
        ]
        return render_table(
            "Storj Nodes", STORJ_GROUPED_HEADERS, STORJ_GROUPED_CAPS,
            STORJ_GROUPED_MINS, display,
            [bool(row["running"]) for row in rows], CYAN, False,
            terminal_width, 0,
        )
    return render_table(
        "Storj Nodes", STORJ_HEADERS, STORJ_CAPS, STORJ_MINS, full_display,
        [bool(row["running"]) for row in rows], CYAN, True, terminal_width, 1,
    )


def sia_table(
    rows: list[dict[str, Any]], wide: bool, terminal_width: int
) -> str:
    full_display = [
        [
            text(row["server"]),
            text(row["node"]),
            text(row["mounts"]),
            compact_ports(row["ports"]),
            text(row["listeners"]),
            text(row["docker_ip"]),
            text(row["compose_file"]),
            text(row["data_path"]),
        ]
        for row in rows
    ]
    if not wide:
        display = [
            [
                text(row["node"]),
                f"Server: {text(row['server'])}\nMounts: {text(row['mounts'])}",
                (
                    f"Ports: {compact_ports(row['ports'])}\n"
                    f"Listen: {text(row['listeners'])}\n"
                    f"Docker: {text(row['docker_ip'])}"
                ),
                (
                    f"Compose: {text(row['compose_file'])}\n"
                    f"Data: {text(row['data_path'])}"
                ),
            ]
            for row in rows
        ]
        return render_table(
            "Sia Hosts", SIA_GROUPED_HEADERS, SIA_GROUPED_CAPS,
            SIA_GROUPED_MINS, display,
            [bool(row["running"]) for row in rows], MAGENTA, False,
            terminal_width, 0,
        )
    return render_table(
        "Sia Hosts", SIA_HEADERS, SIA_CAPS, SIA_MINS, full_display,
        [bool(row["running"]) for row in rows], MAGENTA, True,
        terminal_width, 1,
    )


def main() -> int:
    args = parse_args()
    try:
        root = args.path.expanduser().resolve(strict=True)
        if not root.is_dir():
            raise RuntimeError(f"path is not a directory: {root}")
        containers = docker_inventory()
        hostname = socket.gethostname()
        storj = storj_rows(containers, root, hostname)
        sia = sia_rows(containers, root, hostname)

        selected: dict[str, Any] = {}
        if args.only in ("all", "storj"):
            selected["storj"] = storj
        if args.only in ("all", "sia"):
            selected["sia"] = sia

        if args.json:
            print(json.dumps(selected, ensure_ascii=False, indent=2))
            return 0

        terminal_width = args.width or shutil.get_terminal_size(
            fallback=(140, 24)
        ).columns
        if terminal_width < 60:
            terminal_width = 60
        tables = []
        if "storj" in selected:
            tables.append(storj_table(storj, args.wide, terminal_width))
        if "sia" in selected:
            tables.append(sia_table(sia, args.wide, terminal_width))
        print("\n\n".join(tables))
        return 0
    except (OSError, RuntimeError, subprocess.CalledProcessError, json.JSONDecodeError) as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
