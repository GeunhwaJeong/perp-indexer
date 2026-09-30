#!/usr/bin/env python3
# Copyright (c) 2026 Geunhwa Jeong
# SPDX-License-Identifier: Apache-2.0

"""Extract the event struct layouts of the perpetuals engine.

    scripts/extract_event_layouts.py <perp-dex checkout>

Writes crates/events/layouts/<package>.layout, one file per indexed package, listing every
event struct with its fields in declaration order. BCS has no field names or framing, so the
Rust decoders must declare exactly these fields in exactly this order; the `layouts` test in
crates/events fails when they drift apart.
"""

import re
import subprocess
import sys
from pathlib import Path

PACKAGES = ["perpetuals", "perpetuals_orders", "oracle_aggregator"]
STRUCT = re.compile(r"public struct (\w+)(?:<[^>]*>)?\s+has[^{]*\{([^}]*)\}")


def layouts(source: str):
    source = re.sub(r"//[^\n]*", "", source)
    for name, body in STRUCT.findall(source):
        fields = [f.strip() for f in body.split(",") if f.strip()]
        yield name, [tuple(part.strip() for part in f.split(":", 1)) for f in fields]


def main():
    if len(sys.argv) != 2:
        sys.exit(__doc__)
    root = Path(sys.argv[1]).expanduser()
    commit = subprocess.run(
        ["git", "-C", str(root), "rev-parse", "HEAD"], capture_output=True, text=True, check=True
    ).stdout.strip()
    out_dir = Path(__file__).resolve().parent.parent / "crates" / "events" / "layouts"
    out_dir.mkdir(parents=True, exist_ok=True)
    for package in PACKAGES:
        source = (root / "packages" / package / "sources" / "events.move").read_text()
        lines = [f"# {package}::events at perp-dex {commit}", ""]
        for name, fields in layouts(source):
            lines.append(name)
            lines.extend(f"  {field}: {ty}" for field, ty in fields)
            lines.append("")
        (out_dir / f"{package}.layout").write_text("\n".join(lines))
        print(f"{package}: {sum(1 for _ in layouts(source))} events")


if __name__ == "__main__":
    main()
