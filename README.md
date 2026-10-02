# perp-indexer

Indexer for the perpetuals engine on Haneul. It follows the chain checkpoint by checkpoint and
turns the engine's events into the data a trading front end needs.

## Status

Three of the four planned layers are in place.

The **ledger** (`raw_events`) records every event emitted by the indexed packages, keyed by its
position on chain `(checkpoint, tx_index, event_index)`, with the payload bytes exactly as emitted
and, where a decoder exists, the decoded JSON. It is append-only and idempotent, so it can be
replayed from any checkpoint, and everything built on top of it can be rebuilt from it.

The **state** tables are derived from the chain in order. Markets, accounts, the capabilities
that say who may act for an account, and positions are copied from the objects each transaction
writes, so they hold exactly what the engine holds. Orders, fills, candles (seven resolutions),
funding, collateral transfers, stop/TWAP order tickets and each position's running totals are
built from events. Every batch of checkpoints is written in one transaction together with the
pipeline's watermark, so it is applied exactly once.

The same transaction keeps the **PnL history**. Once per interval (an hour by default), every
account that holds anything is valued: its unallocated balance plus the margin of each of its
positions at the mark price. A tick is the account's value at one exact checkpoint, with what was
transferred into it so far, and the difference between the two, which is what trading, funding
and fees have made or lost. A batch that spans several intervals, as while backfilling, takes
ticks for the last one only: the state in between is no longer there to value.

The **API** (`perp-api`) serves those tables over REST and a WebSocket, in the protocol of the
dYdX v4 indexer, which is what the trading front end is written against. It is a separate
process that only reads the database.

Still to come: decoders for the market-making vault, and the rest of the hardening pass
(continuous reconciliation, replay in CI, load tests).

## Layout

| Path | What it is |
|---|---|
| `crates/types` | Typed decoders for the engine's events and state objects. No chain dependencies. |
| `crates/engine` | The engine's pricing and margin formulas, restated over decimals. The indexer and the API value accounts with the same code. |
| `crates/schema` | Postgres schema and migrations. |
| `crates/indexer` | The indexer binary, built on `haneul-indexer-alt-framework`. |
| `crates/api` | The API binary: REST and WebSocket over the indexer's tables. |
| `scripts/extract_layouts.py` | Regenerates `crates/types/layouts` from an engine checkout. |
| `scripts/reconcile_ledger.py` | Checks the ledger against a full node, event by event. |
| `scripts/check_state.py` | Checks the state tables against the node's objects and against each other. |
| `scripts/check_api.mjs` | Checks a running API against the database, against itself and against the engine. |
| `scripts/localnet/` | A scenario that drives a local network through every engine path, a script that runs the whole pass, and fault checks for the API. |

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

`--pnl-tick-interval-ms` (default one hour) is how often accounts are valued for their PnL
history.

Metrics are served on `--metrics-address` (default `0.0.0.0:9184`).
`perp_indexer_undecoded_events_total` should stay at zero: anything else means the decoders lag
the deployed package.

## The API

```sh
cargo run -p perp-api -- \
  --database-url 'postgres://localhost:5432/perp_indexer?application_name=perp-api' \
  --deployment perp.mainnet.json
```

It listens on `--listen-address` (default `0.0.0.0:3002`): REST under `/v4`, the WebSocket at
`/v4/ws`, and `/health`, which fails while the indexed chain time is more than `--max-lag-secs`
old. Metrics are on `--metrics-address` (default `0.0.0.0:9185`). A read-only database role is
enough, and several API processes can serve one database.

`--deployment` is the deployment description the front end reads (`perp.<network>.json`): the
markets to list with their tickers and clearing house IDs, and the collateral coin. Markets that
are on chain but not in the file are not served.

### How it follows the chain

The API does not hear from the indexer; it reads what the indexer commits. A feed polls the
state pipeline's watermark (`--poll-interval-ms`, 100 by default) and, when it has moved, reads
in one database snapshot everything that changed over the new checkpoints and publishes it as a
round. A subscriber gets a channel's state when it subscribes and the rounds after it, with
message IDs that have no gaps. Books, markets and the tape are answered from memory; candles and
accounts from the database.

Each WebSocket connection is a file descriptor: raise the process's limit (`ulimit -n`) well
above `--ws-max-connections`, and terminate TLS and limit connections and requests per address
in the proxy in front. An idle connection holds a few tens of kilobytes.

Nothing queues without bound. A client that does not keep up with the rounds, stops reading, or
sends more than `--ws-message-rate` messages a second is disconnected and starts over. A peer
that has gone without closing, which looks merely idle, is found by its silence: every
`--ws-ping-interval-secs` it is sent a ping, and it is dropped when one goes unanswered for
`--ws-pong-timeout-secs`. When the feed itself falls more than `--max-round-checkpoints` behind
(an indexer backfill, an outage), it reloads and makes every client start over rather than
replay the gap.

Over REST, skipping rows costs the database as much as returning them, so a paged request may
skip at most `--max-pagination-offset` rows (25,000 by default) and is refused past that; a
client narrows the time range instead. Successful responses say how long they may be reused
(`Cache-Control`: a second for live data, ten for history that grows by the hour, never for the
clock), so that a cache in front absorbs bursts of identical requests.

`perp_api_book_crossed` is 1 while a book's best bid is at or above its best ask. The engine
would have matched those orders, so if it stays set the book has drifted from the chain's.
`perp_api_book_levels` is the depth of each side.

### How the engine's model maps to the protocol

The protocol was designed for dYdX's subaccounts, and the mapping is chosen so that the front
end's own arithmetic stays true.

- **Accounts.** A request names a wallet address and a parent subaccount number. The address's
  accounts are the ones it holds an admin capability for, in the order they were created, and
  the number picks one (almost always there is one, number 0). The parent subaccount's quote
  balance is the account's unallocated collateral. Market number `i` is child subaccount
  `parent + 128 * (i + 1)`: its quote balance is the collateral allocated to the market, plus
  funding accrued and not yet settled, minus what the position cost, so that balance plus
  position value at the mark price is the engine's margin.
- **Prices.** `oraclePrice` is the engine's mark price, which is what positions are valued and
  liquidated at. It, accrued funding and the value of collateral are the only numbers computed
  here rather than copied; `crates/engine` restates the engine's formulas for them.
- **Amounts.** Balances are in USD at the collateral's oracle price. Deposits and withdrawals are
  in coins. The quote asset is presented as `USDC`, the symbol the front end knows.
- **Orders.** An order that rests on the book is a `LIMIT` order whose `id` is the engine's
  order ID. The engine reports no order for what crosses the book, only the fills, so the
  indexer gives each taker fill one: if its transaction posted a single order of the same
  account, market and side, the fill is the part of that order that crossed and counts toward
  its size and filled size; otherwise it becomes a `MARKET` order of its own, already filled,
  with an ID above 2^128 that names nothing on chain. Stop and TWAP tickets commit to their
  details by hash, so they are not presented as orders.
- **Candles.** `fromISO` is inclusive and `toISO` exclusive, as in the dYdX indexer. The chart
  pages backwards by asking for the candles before the oldest one it holds.
- **Fills.** Liquidations appear as `LIQUIDATED` and `LIQUIDATION`, auto-deleveraging and
  settlement as `DELEVERAGED` and `OFFSETTING`. The tape (`v4_trades`) has each match once, with
  the taker's side.
- **Funding.** Rates are per hour, as fractions of position value. A settlement that moved
  nothing is not listed as a payment.
- **PnL history.** `/v4/pnl` serves the ticks the indexer takes, each reported at the start of
  its interval; with `daily=true`, the first tick of each UTC day. Equity is the account's
  equity as the subaccount endpoint reports it, over every market on chain. Net transfers are
  the coins deposited less the coins withdrawn, at the collateral's price when the tick was
  taken. `/v4/historical-pnl` serves the same ticks in the older shape.
- **Not kept.** Trading rewards answer empty. `startingOpenInterest` of a candle is `0`.

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
fills against its size, transfers against an account's balance and its running net transfers,
the top of the book and the open interest against the market, candles against trades, and the
PnL ticks against the runs that took them.

`scripts/check_api.mjs` checks the API. It follows every channel the way a client does and
checks the protocol (message IDs, acknowledgements, the error texts clients match on), that a
client which followed every update holds the same state as one that subscribes afterwards and as
REST reports, that every object has the fields the front end requires, that responses equal what
the tables hold (the PnL history included, whose latest tick must equal the account's equity),
and, given the engine's own view from the scenario below, that the mark price, balances and
accrued funding equal the engine's:

```sh
scripts/check_api.mjs --api http://127.0.0.1:3002 --deployment perp.localnet.json \
  --database-url postgres://localhost:5432/perp_indexer
```

## Localnet pass

`scripts/localnet/run_all.sh` starts a throwaway network, publishes the engine from a copy of its
checkout, starts the indexer at the tip and the API over it, subscribes a client to every
channel and then, while they all follow the chain, runs `scripts/localnet/scenario.py`:
administrator parameter changes, integrator fees, stop and TWAP tickets through every step of
their life, a liquidation whose bad debt is socialized, a funding update that is settled for one
account and left accrued for another, and an auto-deleveraging. It ends with the three checks
above.

```sh
cargo build
scripts/localnet/run_all.sh <work dir> <engine checkout copy> <haneul binary> postgres://localhost:5432/perp_indexer_localnet
```

With `HOLD=<path>` the stack stays up after the checks until that file appears. That is the
state `scripts/localnet/faults.mjs` needs: it floods the API, leaves its pings unanswered,
stalls a reader, freezes the process until its feed falls behind and drops its database
connections, and checks that each ends the way it should. The pass takes PnL ticks every five
seconds (`PNL_TICK_INTERVAL_MS`) so that the history has something in it.

```sh
scripts/localnet/faults.mjs --api-binary target/debug/perp-api \
  --deployment <work dir>/deployment.json --database-url postgres://localhost:5432/perp_indexer_localnet
```

The engine's own suite (`e2e/localnet_e2e.py` in its checkout) covers the rest: settlement,
funding, fee withdrawal and the vault. Run the indexer over a network it ran on and apply the same
checks.

## Dependencies

Haneul is pinned to a single commit in `Cargo.toml`, so the framework, the checkpoint types and
the node protocol stay in step. Move all of the pinned entries together.
