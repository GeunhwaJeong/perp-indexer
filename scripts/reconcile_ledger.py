#!/usr/bin/env python3
# Copyright (c) 2026 Geunhwa Jeong
# SPDX-License-Identifier: Apache-2.0

"""Reconcile the `raw_events` ledger against a full node.

    scripts/reconcile_ledger.py --database-url postgres://.../perp_indexer \\
        --grpc 127.0.0.1:9000 --package perpetuals=0x... [--package ...] [--first N] [--last N]

Reads every checkpoint in the range from the node over gRPC, takes the events whose type lives
in one of the given packages, and checks them against the ledger in both directions:

  - every event on chain has exactly one row, and the ledger has no row the chain lacks;
  - digest, event type and payload bytes are identical;
  - for decoded rows, `data` equals the node's own rendering of the event.

The last check compares two independent decoders: the node renders events from the layouts
published on chain, the indexer from the layouts declared in crates/types.

Needs grpcurl and psql. Pass --plaintext=false for a TLS endpoint. Exits non-zero on any
mismatch.
"""

import argparse
import base64
import json
import subprocess
import sys

READ_MASK = {"paths": ["sequence_number", "transactions.digest", "transactions.events"]}


def normalize_address(address):
    return "0x" + address.removeprefix("0x").rjust(64, "0")


def grpc(args, method, request):
    cmd = ["grpcurl", "-max-msg-sz", str(256 * 1024 * 1024)]
    if args.plaintext:
        cmd.append("-plaintext")
    cmd += ["-d", json.dumps(request), args.grpc, f"haneul.rpc.v2.{method}"]
    out = subprocess.run(cmd, capture_output=True, text=True)
    if out.returncode != 0:
        sys.exit(f"{method} failed: {out.stderr.strip()}")
    return json.loads(out.stdout)


def chain_events(args, packages, first, last):
    """Yields (key, event) for every event of the indexed packages, in chain order."""
    for sequence_number in range(first, last + 1):
        request = {"sequence_number": sequence_number, "read_mask": READ_MASK}
        checkpoint = grpc(args, "LedgerService/GetCheckpoint", request)["checkpoint"]
        for tx_index, tx in enumerate(checkpoint.get("transactions", [])):
            for event_index, event in enumerate(tx.get("events", {}).get("events", [])):
                address = normalize_address(event["eventType"].split("::", 1)[0])
                if address in packages:
                    yield (sequence_number, tx_index, event_index), tx["digest"], event


def ledger_rows(args, first, last):
    query = f"""
        SELECT coalesce(json_agg(json_build_object(
            'key', json_build_array(checkpoint, tx_index, event_index),
            'tx_digest', tx_digest,
            'event_type', event_type,
            'bcs', encode(bcs, 'base64'),
            'data', data,
            'decode_error', decode_error
        )), '[]')
        FROM raw_events WHERE checkpoint BETWEEN {first} AND {last}
    """
    out = subprocess.run(
        ["psql", args.database_url, "-AtXc", query], capture_output=True, text=True
    )
    if out.returncode != 0:
        sys.exit(f"psql failed: {out.stderr.strip()}")
    return {tuple(row["key"]): row for row in json.loads(out.stdout)}


def watermark(args):
    query = "SELECT checkpoint_hi_inclusive FROM watermarks WHERE pipeline = 'raw_events'"
    out = subprocess.run(
        ["psql", args.database_url, "-AtXc", query], capture_output=True, text=True
    )
    if out.returncode != 0 or not out.stdout.strip():
        sys.exit(f"no raw_events watermark: {out.stderr.strip()}")
    return int(out.stdout.strip())


def canonical_type(event_type):
    """Pads every address in a type string to 32 bytes, as the ledger stores them."""
    out, i = [], 0
    while i < len(event_type):
        if event_type.startswith("0x", i):
            j = i + 2
            while j < len(event_type) and event_type[j] in "0123456789abcdefABCDEF":
                j += 1
            out.append(normalize_address(event_type[i:j]))
            i = j
        else:
            out.append(event_type[i])
            i += 1
    return "".join(out)


def same_value(ours, theirs):
    """Compares the indexer's JSON with the node's rendering of the same Move value.

    The two differ only in notation: the node writes u64 as a string, byte vectors as base64
    and a TypeName as `{"name": ...}`; the indexer writes u64 as a number, bytes as 0x hex and
    a TypeName as a bare string.
    """
    if isinstance(ours, dict):
        return (
            isinstance(theirs, dict)
            and ours.keys() == theirs.keys()
            and all(same_value(ours[k], theirs[k]) for k in ours)
        )
    if isinstance(ours, list):
        return (
            isinstance(theirs, list)
            and len(ours) == len(theirs)
            and all(same_value(a, b) for a, b in zip(ours, theirs))
        )
    if isinstance(theirs, dict) and theirs.keys() == {"name"}:
        return ours == theirs["name"]
    if ours is None or isinstance(ours, bool):
        return ours == theirs
    if isinstance(ours, (int, float)):
        return str(ours) == str(theirs)
    if isinstance(ours, str) and isinstance(theirs, str):
        if ours == theirs:
            return True
        if ours.startswith("0x") and theirs.startswith("0x"):
            return normalize_address(ours) == normalize_address(theirs)
        if ours.startswith("0x"):
            try:
                return bytes.fromhex(ours[2:]) == base64.b64decode(theirs, validate=True)
            except ValueError:
                return False
    return False


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--database-url", required=True)
    parser.add_argument("--grpc", required=True, help="host:port of the full node")
    parser.add_argument("--plaintext", default=True, type=lambda v: v.lower() != "false")
    parser.add_argument("--package", action="append", required=True, metavar="NAME=ADDRESS")
    parser.add_argument("--first", type=int, default=0)
    parser.add_argument("--last", type=int, help="defaults to the raw_events watermark")
    args = parser.parse_args()

    packages = {normalize_address(p.split("=", 1)[1]) for p in args.package}
    last = args.last if args.last is not None else watermark(args)
    rows = ledger_rows(args, args.first, last)

    problems, seen, decoded, by_name = [], set(), 0, {}
    for key, digest, event in chain_events(args, packages, args.first, last):
        seen.add(key)
        row = rows.get(key)
        name = event["eventType"].split("<", 1)[0].split("::", 2)[2]
        by_name[name] = by_name.get(name, 0) + 1
        if row is None:
            problems.append(f"{key}: {name} is on chain but not in the ledger")
            continue
        if row["tx_digest"] != digest:
            problems.append(f"{key}: digest {row['tx_digest']} != {digest}")
        if row["event_type"] != canonical_type(event["eventType"]):
            problems.append(f"{key}: type {row['event_type']} != {event['eventType']}")
        if base64.b64decode(row["bcs"]) != base64.b64decode(event["contents"]["value"]):
            problems.append(f"{key}: {name} payload bytes differ")
        if row["decode_error"] is not None:
            problems.append(f"{key}: {name} failed to decode: {row['decode_error']}")
        elif row["data"] is not None:
            decoded += 1
            if not same_value(row["data"], event.get("json")):
                problems.append(
                    f"{key}: {name} decodes differently\n"
                    f"    ledger: {json.dumps(row['data'], sort_keys=True)}\n"
                    f"    node:   {json.dumps(event.get('json'), sort_keys=True)}"
                )
    for key in sorted(rows.keys() - seen):
        problems.append(f"{key}: in the ledger but not on chain")

    print(f"checkpoints {args.first}..{last}: {len(seen)} events on chain, {len(rows)} in the ledger, "
          f"{decoded} decoded rows compared with the node's rendering")
    for name in sorted(by_name):
        print(f"  {by_name[name]:6}  {name}")
    if problems:
        print(f"\n{len(problems)} mismatches:")
        for problem in problems[:50]:
            print("  " + problem)
        sys.exit(1)
    print("ledger matches the chain")


if __name__ == "__main__":
    main()
