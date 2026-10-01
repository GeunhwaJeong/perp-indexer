#!/usr/bin/env python3
# Copyright (c) 2026 Geunhwa Jeong
# SPDX-License-Identifier: Apache-2.0

"""Extract the struct layouts the indexer decodes from the perpetuals engine.

    scripts/extract_layouts.py <perp-dex checkout>

Writes crates/types/layouts/<name>.layout: one file per package for its events, and
`objects.layout` for the on-chain objects the indexer reads state from. Each file lists the
structs with their fields in declaration order. BCS has no field names or framing, so the Rust
decoders must declare exactly these fields in exactly this order; the `layouts` test in
crates/types fails when they drift apart.
"""

import re
import subprocess
import sys
from pathlib import Path

# Layout file -> [(source file under packages/, structs to take, or None for all of them)].
LAYOUTS = {
    "perpetuals": [("perpetuals/sources/events.move", None)],
    "perpetuals_orders": [("perpetuals_orders/sources/events.move", None)],
    "oracle_aggregator": [("oracle_aggregator/sources/events.move", None)],
    "objects": [
        ("position/sources/position.move", ["Position"]),
        ("perpetuals/sources/keys.move", ["PositionKey"]),
        (
            "perpetuals/sources/market.move",
            ["CoreParams", "FeesParams", "TwapParams", "LimitsParams", "MarketParams", "MarketState"],
        ),
        ("perpetuals/sources/orderbook.move", ["Orderbook"]),
        ("perpetuals/sources/clearing_house.move", ["ClearingHouse"]),
        ("perpetuals/sources/account.move", ["Account"]),
        ("authority_cap/sources/authority.move", ["AuthorityCap"]),
    ],
}
STRUCT = re.compile(r"public struct (\w+)(?:<[^>]*>)?\s+has[^{]*\{([^}]*)\}")


def layouts(source: str):
    source = re.sub(r"//[^\n]*", "", source)
    for name, body in STRUCT.findall(source):
        fields = [f.strip() for f in body.split(",") if f.strip()]
        # Backticks escape field names that are Move keywords.
        yield name, [tuple(part.strip().strip("`") for part in f.split(":", 1)) for f in fields]


def main():
    if len(sys.argv) != 2:
        sys.exit(__doc__)
    root = Path(sys.argv[1]).expanduser()
    commit = subprocess.run(
        ["git", "-C", str(root), "rev-parse", "HEAD"], capture_output=True, text=True, check=True
    ).stdout.strip()
    out_dir = Path(__file__).resolve().parent.parent / "crates" / "types" / "layouts"
    out_dir.mkdir(parents=True, exist_ok=True)
    for name, sources in LAYOUTS.items():
        lines = [f"# {name} at perp-dex {commit}", ""]
        count = 0
        for path, wanted in sources:
            found = dict(layouts((root / "packages" / path).read_text()))
            for struct in wanted if wanted is not None else found:
                if struct not in found:
                    sys.exit(f"{path}: no struct {struct}")
                lines.append(struct)
                lines.extend(f"  {field}: {ty}" for field, ty in found[struct])
                lines.append("")
                count += 1
        (out_dir / f"{name}.layout").write_text("\n".join(lines))
        print(f"{name}: {count} structs")


if __name__ == "__main__":
    main()
