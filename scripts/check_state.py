#!/usr/bin/env python3
# Copyright (c) 2026 Geunhwa Jeong
# SPDX-License-Identifier: Apache-2.0

"""Check the derived state against the chain and against itself.

    scripts/check_state.py --database-url postgres://.../perp_indexer --grpc 127.0.0.1:9000

The state tables are built two ways that must agree: positions, markets and accounts are copied
from on-chain objects, while orders, fills and candles are built from events. So the checks are

  - every position, market and account row equals the node's own rendering of its object;
  - a position's resting quantities and pending order count equal its open orders;
  - a market's best prices equal the top of its open orders;
  - a market's open interest equals the long positions;
  - a position's size equals the sum of its fills;
  - an account's free collateral equals its deposits, withdrawals and allocations;
  - orders and candles are internally consistent, and fills match the events they came from.

Needs grpcurl and psql. Exits non-zero on any mismatch.
"""

import argparse
import json
import subprocess
import sys
from decimal import Decimal

ONE = Decimal(10) ** 18
B9 = Decimal(10) ** 9


def psql(args, query):
    out = subprocess.run(["psql", args.database_url, "-AtXc", query], capture_output=True, text=True)
    if out.returncode != 0:
        sys.exit(f"psql failed: {out.stderr.strip()}\n{query}")
    return out.stdout


def rows(args, query):
    text = psql(args, f"SELECT coalesce(json_agg(q), '[]') FROM ({query}) q")
    return json.loads(text, parse_float=Decimal)


def grpc_object(args, object_id):
    cmd = ["grpcurl"]
    if args.plaintext:
        cmd.append("-plaintext")
    request = {"object_id": object_id, "read_mask": {"paths": ["json", "owner"]}}
    cmd += ["-d", json.dumps(request), args.grpc, "haneul.rpc.v2.LedgerService/GetObject"]
    out = subprocess.run(cmd, capture_output=True, text=True)
    if out.returncode != 0:
        sys.exit(f"GetObject {object_id} failed: {out.stderr.strip()}")
    return json.loads(out.stdout)["object"]


def signed(value):
    """An ifixed value from the node's unsigned rendering."""
    value = int(value)
    return Decimal(value - (1 << 256) if value >= (1 << 255) else value) / ONE


def opt(value):
    return None if value is None else int(value)


class Checks:
    def __init__(self):
        self.problems = []
        self.count = 0

    def eq(self, what, got, want):
        self.count += 1
        if got != want:
            self.problems.append(f"{what}: got {got}, want {want}")

    def report(self):
        print(f"{self.count} checks, {len(self.problems)} mismatches")
        for problem in self.problems[:50]:
            print("  " + problem)
        return not self.problems


def check_objects(args, c):
    for row in rows(args, "SELECT * FROM positions"):
        what = f"position {row['market']}/{row['account_id']}"
        obj = grpc_object(args, row["object_id"])
        c.eq(f"{what} owner", obj["owner"]["address"], row["market"])
        c.eq(f"{what} account", int(obj["json"]["name"]["account_id"]), row["account_id"])
        value = obj["json"]["value"]
        for column, field in [
            ("collateral", "collateral"),
            ("base", "base_asset_amount"),
            ("quote_notional", "quote_asset_notional_amount"),
            ("cum_funding_rate_long", "cum_funding_rate_long"),
            ("cum_funding_rate_short", "cum_funding_rate_short"),
            ("asks_quantity", "asks_quantity"),
            ("bids_quantity", "bids_quantity"),
            ("initial_margin_ratio", "initial_margin_ratio"),
        ]:
            c.eq(f"{what} {column}", Decimal(row[column]), signed(value[field]))
        c.eq(f"{what} pending_orders", row["pending_orders"], int(value["pending_orders"]))

    for row in rows(args, "SELECT * FROM markets"):
        what = f"market {row['market']}"
        obj = grpc_object(args, row["market"])["json"]
        core = obj["market_params"]["core_params"]
        fees = obj["market_params"]["fees_params"]
        state = obj["market_state"]
        book = obj["orderbook"]
        c.eq(f"{what} version", row["version"], int(obj["version"]))
        c.eq(f"{what} paused", row["paused"], int(obj["paused"]))
        c.eq(f"{what} lot_size", Decimal(row["lot_size"]), Decimal(core["lot_size"]) / B9)
        c.eq(f"{what} tick_size", Decimal(row["tick_size"]), Decimal(core["tick_size"]) / B9)
        c.eq(f"{what} maker_fee", Decimal(row["maker_fee"]), signed(fees["maker_fee"]))
        c.eq(f"{what} taker_fee", Decimal(row["taker_fee"]), signed(fees["taker_fee"]))
        c.eq(f"{what} margin_ratio_initial", Decimal(row["margin_ratio_initial"]), signed(core["margin_ratio_initial"]))
        for column in ["cum_funding_rate_long", "cum_funding_rate_short", "premium_twap", "spread_twap", "open_interest", "fees_accrued"]:
            c.eq(f"{what} {column}", Decimal(row[column]), signed(state[column]))
        c.eq(f"{what} funding_last_upd_ms", row["funding_last_upd_ms"], int(state["funding_last_upd_ms"]))
        c.eq(f"{what} order_counter", int(Decimal(row["order_counter"])), int(book["counter"]))
        for column in ["best_ask_price", "best_bid_price"]:
            want = opt(book[column])
            got = None if row[column] is None else int(Decimal(row[column]) * B9)
            c.eq(f"{what} {column}", got, want)

    for row in rows(args, "SELECT * FROM accounts"):
        what = f"account {row['account_id']}"
        obj = grpc_object(args, row["object_id"])["json"]
        c.eq(f"{what} id", int(obj["account_id"]), row["account_id"])
        c.eq(f"{what} collateral", int(Decimal(row["collateral"])), int(obj["collateral"]))
        c.eq(f"{what} creator set", row["creator"] is not None, True)


def check_invariants(args, c):
    # Resting orders per position, from the events, against the object's own counters.
    for row in rows(args, """
        SELECT p.market, p.account_id, p.asks_quantity, p.bids_quantity, p.pending_orders,
               coalesce(sum(o.remaining) FILTER (WHERE o.is_ask), 0) AS asks,
               coalesce(sum(o.remaining) FILTER (WHERE NOT o.is_ask), 0) AS bids,
               count(o.order_id) AS open_orders
        FROM positions p
        LEFT JOIN orders o ON o.market = p.market AND o.account_id = p.account_id AND o.status = 'open'
        GROUP BY p.market, p.account_id, p.asks_quantity, p.bids_quantity, p.pending_orders
    """):
        what = f"position {row['market']}/{row['account_id']}"
        c.eq(f"{what} asks_quantity = open asks", Decimal(row["asks_quantity"]), Decimal(row["asks"]))
        c.eq(f"{what} bids_quantity = open bids", Decimal(row["bids_quantity"]), Decimal(row["bids"]))
        c.eq(f"{what} pending_orders = open orders", row["pending_orders"], row["open_orders"])

    # The top of the book and the open interest, from the events, against the market object.
    for row in rows(args, """
        SELECT m.market, m.best_ask_price, m.best_bid_price, m.open_interest,
               (SELECT min(price) FROM orders WHERE market = m.market AND status = 'open' AND is_ask) AS best_ask,
               (SELECT max(price) FROM orders WHERE market = m.market AND status = 'open' AND NOT is_ask) AS best_bid,
               (SELECT coalesce(sum(greatest(base, 0)), 0) FROM positions WHERE market = m.market) AS longs
        FROM markets m
    """):
        what = f"market {row['market']}"
        for column, want in [("best_ask_price", row["best_ask"]), ("best_bid_price", row["best_bid"])]:
            got = None if row[column] is None else Decimal(row[column])
            c.eq(f"{what} {column} = top of open orders", got, None if want is None else Decimal(want))
        c.eq(f"{what} open_interest = long positions", Decimal(row["open_interest"]), Decimal(row["longs"]))

    # A position's size is what its fills add up to.
    for row in rows(args, """
        SELECT p.market, p.account_id, p.base,
               coalesce(sum(CASE WHEN f.is_ask THEN -f.size ELSE f.size END), 0) AS filled
        FROM positions p
        LEFT JOIN fills f ON f.market = p.market AND f.account_id = p.account_id
        GROUP BY p.market, p.account_id, p.base
    """):
        c.eq(f"position {row['market']}/{row['account_id']} base = sum of fills",
             Decimal(row["base"]), Decimal(row["filled"]))

    # Free collateral is what moved in and out of the account.
    for row in rows(args, """
        SELECT a.account_id, a.collateral,
               coalesce(sum(CASE WHEN t.kind IN ('deposit', 'deallocate', 'settlement') THEN t.amount
                                 ELSE -t.amount END), 0) AS net
        FROM accounts a
        LEFT JOIN collateral_transfers t ON t.account_id = a.account_id
        GROUP BY a.account_id, a.collateral
    """):
        c.eq(f"account {row['account_id']} collateral = net transfers", Decimal(row["collateral"]), Decimal(row["net"]))

    # Orders and candles are consistent with themselves and with the fills.
    bad = rows(args, """
        SELECT market, order_id FROM orders
        WHERE remaining <> size - filled - canceled
           OR (status = 'open') <> (remaining > 0)
           OR (status = 'canceled' AND canceled = 0)
           OR (status = 'filled' AND (canceled <> 0 OR filled <> size))
    """)
    c.eq("orders with inconsistent size, remaining and status", len(bad), 0)
    for row in rows(args, """
        SELECT c.market, c.resolution_ms, sum(c.base_volume) AS volume, sum(c.trades) AS trades,
               (SELECT coalesce(sum(size), 0) FROM fills f
                 WHERE f.market = c.market AND f.kind = 'trade' AND f.liquidity = 'maker') AS traded,
               (SELECT count(*) FROM fills f
                 WHERE f.market = c.market AND f.kind = 'trade' AND f.liquidity = 'maker') AS fills
        FROM candles c GROUP BY c.market, c.resolution_ms
    """):
        what = f"candles {row['market']} @{row['resolution_ms']}"
        c.eq(f"{what} volume = traded size", Decimal(row["volume"]), Decimal(row["traded"]))
        c.eq(f"{what} trades = maker fills", row["trades"], row["fills"])

    # Every fill-producing event produced its fills.
    counts = {r["k"]: r["n"] for r in rows(args, "SELECT liquidity || '/' || kind AS k, count(*) AS n FROM fills GROUP BY 1")}
    events = {r["name"]: r["n"] for r in rows(args, """
        SELECT name, sum(CASE WHEN name = 'FilledMakerOrders'
                              THEN (SELECT count(*) FROM jsonb_array_elements(data->'events') e WHERE (e->>'filled_size') <> '0')
                              WHEN name = 'ClosedPositionAtSettlementPrices'
                              THEN CASE WHEN (data->>'base_asset_amount') <> '0' THEN 1 ELSE 0 END
                              ELSE 1 END) AS n
        FROM raw_events WHERE package = 'perpetuals' GROUP BY name
    """)}
    for kind, name in [("maker/trade", "FilledMakerOrders"), ("taker/trade", "FilledTakerOrder"),
                       ("taker/liquidated", "LiquidatedPosition"), ("taker/liquidation", "PerformedLiquidation"),
                       ("taker/settlement", "ClosedPositionAtSettlementPrices")]:
        c.eq(f"fills {kind} = events {name}", counts.get(kind, 0), int(events.get(name, 0)))
    c.eq("fills taker/adl = 2 x PerformedADL", counts.get("taker/adl", 0), 2 * int(events.get("PerformedADL", 0)))
    c.eq("order tickets = created ticket events",
         rows(args, "SELECT count(*) AS n FROM order_tickets")[0]["n"],
         int(rows(args, "SELECT count(*) AS n FROM raw_events WHERE name IN ('CreatedStopOrderTicket', 'CreatedTWAPOrderTicket')")[0]["n"]))


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--database-url", required=True)
    parser.add_argument("--grpc", required=True, help="host:port of the full node")
    parser.add_argument("--plaintext", default=True, type=lambda v: v.lower() != "false")
    args = parser.parse_args()

    watermarks = {r["pipeline"]: r["checkpoint_hi_inclusive"] for r in rows(args, "SELECT pipeline, checkpoint_hi_inclusive FROM watermarks")}
    print(f"watermarks: {watermarks}")
    c = Checks()
    check_objects(args, c)
    check_invariants(args, c)
    sys.exit(0 if c.report() else 1)


if __name__ == "__main__":
    main()
