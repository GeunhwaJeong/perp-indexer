# perp-indexer

Indexer for the perpetuals engine on Haneul. It follows the chain checkpoint by checkpoint and
turns the engine's events into the data a trading front end needs.

## Status

Two of the four planned layers are in place.

The **ledger** (`raw_events`) records every event emitted by the indexed packages, keyed by its
position on chain `(checkpoint, tx_index, event_index)`, with the payload bytes exactly as emitted
and, where a decoder exists, the decoded JSON. It is append-only and idempotent, so it can be
replayed from any checkpoint, and everything built on top of it can be rebuilt from it.

The **state** tables are derived from the chain in order. Markets, accounts and positions are
copied from the objects each transaction writes, so they hold exactly what the engine holds.
Orders, fills, candles (seven resolutions), funding, collateral transfers and stop/TWAP order
tickets are built from events. Every batch of checkpoints is written in one transaction together
with the pipeline's watermark, so it is applied exactly once.

Still to come: the REST and WebSocket API, and decoders for the market-making vault.

## Layout

| Path | What it is |
|---|---|
| `crates/types` | Typed decoders for the engine's events and state objects. No chain dependencies. |
| `crates/schema` | Postgres schema and migrations. |
| `crates/indexer` | The indexer binary, built on `haneul-indexer-alt-framework`. |
| `scripts/extract_layouts.py` | Regenerates `crates/types/layouts` from an engine checkout. |
| `scripts/reconcile_ledger.py` | Checks the ledger against a full node, event by event. |
| `scripts/check_state.py` | Checks the state tables against the node's objects and against each other. |
| `scripts/localnet/` | A scenario that drives a local network through every engine path, and a script that runs the whole pass. |

## Running

The indexer needs Postgres and a Haneul full node that serves gRPC. It applies its own
migrations on start.

```sh
createdb perp_indexer

cargo run -p perp-indexer -- \
  --database-url postgres://localhost:5432/perp_indexer \
  --rpc-api-url http://127.0.0.1:9000 \
  --streaming-url http://127.0.0.1:9000 \
  --package perpetuals=0x... \
  --package perpetuals_orders=0x... \
  --package oracle_aggregator=0x... \
  --package market_making_vault=0x...
```

`--rpc-api-url` is where checkpoints are fetched while catching up and `--streaming-url` is the
subscription used once the indexer is at the tip; they can be the same node. Packages can also be
given as `PERP_PACKAGES=name=0x...,name=0x...`.

An event type keeps the address of the package version that first defined it, so after an
upgrade that adds events, list the new version's address under the same name.

Events of `perpetuals`, `perpetuals_orders` and `oracle_aggregator` are decoded. A package under
any other name is recorded as raw bytes, which keeps its history in the ledger until decoders for
it are written.

Metrics are served on `--metrics-address` (default `0.0.0.0:9184`).
`perp_indexer_undecoded_events_total` should stay at zero: anything else means the decoders lag
the deployed package.

## Keeping the decoders honest

Event payloads are BCS, which is positional: a field that is missing, reordered or mistyped
shifts every value after it. Two checks guard against that.

`cargo test` compares the Rust declarations in `crates/types` with the field layouts extracted
from the engine's Move sources. After the engine changes, refresh them:

```sh
scripts/extract_layouts.py ~/perp-dex
```

`scripts/reconcile_ledger.py` compares a running indexer with the chain. It reads every
checkpoint from the node, and checks that each event has exactly one ledger row with the same
digest, type and bytes, and that the decoded JSON equals the node's own rendering of the event:

```sh
scripts/reconcile_ledger.py \
  --database-url postgres://localhost:5432/perp_indexer \
  --grpc 127.0.0.1:9000 \
  --package perpetuals=0x... --package oracle_aggregator=0x...
```

`scripts/check_state.py` does the same for the state tables. Positions, markets and accounts are
compared with the node's rendering of their objects, and the tables built from events are
compared with them: open orders against a position's resting quantities and pending order count,
fills against its size, transfers against an account's balance, the top of the book and the open
interest against the market, candles against trades.

## Localnet pass

`scripts/localnet/run_all.sh` starts a throwaway network, publishes the engine from a copy of its
checkout, starts the indexer at the tip and, while it streams, runs `scripts/localnet/scenario.py`:
administrator parameter changes, integrator fees, stop and TWAP tickets through every step of
their life, a liquidation whose bad debt is socialized and an auto-deleveraging. It ends with the
two checks above.

```sh
scripts/localnet/run_all.sh <work dir> <engine checkout copy> <haneul binary> postgres://localhost:5432/perp_indexer_localnet
```

The engine's own suite (`e2e/localnet_e2e.py` in its checkout) covers the rest: settlement,
funding, fee withdrawal and the vault. Run the indexer over a network it ran on and apply the same
two checks.

## Dependencies

Haneul is pinned to a single commit in `Cargo.toml`, so the framework, the checkpoint types and
the node protocol stay in step. Move all of the pinned entries together.
