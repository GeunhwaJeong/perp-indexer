#!/usr/bin/env node
// Copyright (c) 2026 Geunhwa Jeong
// SPDX-License-Identifier: Apache-2.0

// Checks a running API against the database it reads, against itself, and against the chain.
//
//   scripts/check_api.mjs --api http://127.0.0.1:3002 --database-url postgres://... \
//       --deployment <file> [--ready <file>] [--stop <file>] [--probe <file>]
//
// It subscribes to every channel and keeps the state a client would keep. With `--ready` it
// writes that file once subscribed, and with `--stop` it then follows the stream until that
// file appears, so it can be started before a scenario and finished after it. Then it checks:
//
//   - the protocol: message IDs without gaps, acknowledgements, the error texts clients match on;
//   - the stream: a client that followed every update holds the same state as one that
//     subscribes afterwards, and as the REST endpoints report;
//   - the contract: every object has the fields the front end requires, of the right types;
//   - the database: REST responses equal what the tables hold;
//   - the chain: with `--probe` (written by scripts/localnet/scenario.py), the mark price and
//     every account's balances equal what the engine itself reports.
//
// Needs Node 22 or later (built-in WebSocket and fetch) and psql.

import { execFileSync } from 'node:child_process';
import { existsSync, readFileSync, writeFileSync } from 'node:fs';
import { parseArgs } from 'node:util';

const { values: args } = parseArgs({
  options: {
    api: { type: 'string', default: 'http://127.0.0.1:3002' },
    'database-url': { type: 'string' },
    deployment: { type: 'string' },
    ready: { type: 'string' },
    stop: { type: 'string' },
    probe: { type: 'string' },
  },
});
if (!args['database-url'] || !args.deployment) {
  console.error('usage: check_api.mjs --api <url> --database-url <url> --deployment <file>');
  process.exit(2);
}

const API = args.api.replace(/\/$/, '');
const WS = `${API.replace(/^http/, 'ws')}/v4/ws`;
const deployment = JSON.parse(readFileSync(args.deployment, 'utf8'));
const TICKERS = Object.keys(deployment.markets);
const DECIMALS = deployment.collateral.decimals;
// The address whose accounts are followed: the one the scenario trades from.
const OWNER = deployment.owner;
const PARENTS = [0, 1, 2, 3];
const RESOLUTIONS = ['1MIN', '1DAY'];

// ------------------------------------------------------------------------------- reporting

const results = [];
function check(name, ok, detail = '') {
  results.push([name, Boolean(ok)]);
  if (!ok) console.log(`FAIL ${name}${detail ? `: ${detail}` : ''}`);
}

function same(name, got, want) {
  const [g, w] = [JSON.stringify(sorted(got)), JSON.stringify(sorted(want))];
  check(name, g === w, g === w ? '' : `\n  got  ${clip(g)}\n  want ${clip(w)}`);
}

const clip = (s) => (s.length > 700 ? `${s.slice(0, 700)}…` : s);

/** A copy with object keys in order, so that two values compare as text. */
function sorted(value) {
  if (Array.isArray(value)) return value.map(sorted);
  if (value && typeof value === 'object') {
    return Object.fromEntries(
      Object.keys(value)
        .sort()
        .map((k) => [k, sorted(value[k])])
    );
  }
  return value;
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// --------------------------------------------------------------------------------- decimals

/** A decimal string as an integer number of 1e-18. */
function fixed(text) {
  const m = /^(-?)(\d+)(?:\.(\d+))?$/.exec(String(text));
  if (!m) throw new Error(`not a decimal: ${text}`);
  const frac = (m[3] ?? '').padEnd(18, '0').slice(0, 18);
  const n = BigInt(m[2]) * 10n ** 18n + BigInt(frac);
  return m[1] ? -n : n;
}

const abs = (n) => (n < 0n ? -n : n);

/** Whether two decimal strings are within `tolerance` (a decimal string) of each other. */
const near = (a, b, tolerance) => abs(fixed(a) - fixed(b)) <= fixed(tolerance);

// ---------------------------------------------------------------------------------- sources

function sql(query) {
  const out = execFileSync(
    'psql',
    [args['database-url'], '-AtXc', `SELECT coalesce(json_agg(t), '[]') FROM (${query}) t`],
    { encoding: 'utf8', maxBuffer: 256 * 1024 * 1024 }
  );
  return JSON.parse(out);
}

async function rest(path, expectStatus = 200) {
  const response = await fetch(`${API}${path}`);
  const body = await response.json();
  if (response.status !== expectStatus) {
    throw new Error(`${path}: status ${response.status}, body ${JSON.stringify(body)}`);
  }
  return body;
}

// ------------------------------------------------------------------------------- the stream

/** A WebSocket client that keeps what the front end keeps for each subscription. */
class Stream {
  constructor(name) {
    this.name = name;
    this.messages = 0;
    this.lastId = -1;
    this.gaps = [];
    this.errors = [];
    this.acks = new Map();
    this.waiting = new Map();
    this.lastMessageAt = Date.now();

    this.markets = null;
    this.books = {};
    this.trades = {};
    this.candles = {};
    this.accounts = {};
    // Everything a subaccount subscription was sent, to check what was pushed and when.
    this.accountLog = {};
  }

  open() {
    return new Promise((resolve, reject) => {
      this.socket = new WebSocket(WS);
      this.socket.addEventListener('error', (e) => reject(new Error(`${this.name}: ${e.message}`)));
      this.socket.addEventListener('close', (e) => {
        this.closed = { code: e.code, reason: e.reason };
      });
      this.socket.addEventListener('message', (event) => {
        const message = JSON.parse(event.data);
        this.receive(message);
        if (message.type === 'connected') resolve(message);
      });
    });
  }

  close() {
    this.socket.close();
  }

  send(message) {
    this.socket.send(JSON.stringify(message));
  }

  /** Sends a subscribe and resolves with the `subscribed` message (or the error). */
  subscribe(channel, id) {
    const key = `${channel}/${id ?? ''}`;
    const answered = new Promise((resolve) => this.waiting.set(key, resolve));
    this.send({ type: 'subscribe', channel, id, batched: true });
    return answered;
  }

  /** Resolves with the next message of a type that arrives. */
  next(type) {
    return new Promise((resolve) => this.waiting.set(`type:${type}`, resolve));
  }

  /** Resolves once no message has arrived for `ms`. */
  async quiet(ms) {
    while (Date.now() - this.lastMessageAt < ms) await sleep(50);
  }

  receive(message) {
    this.messages += 1;
    this.lastMessageAt = Date.now();
    if (message.message_id !== this.lastId + 1) this.gaps.push([this.lastId, message.message_id]);
    this.lastId = message.message_id;

    const typed = this.waiting.get(`type:${message.type}`);
    if (typed) {
      this.waiting.delete(`type:${message.type}`);
      typed(message);
    }
    const key = `${message.channel}/${message.id ?? ''}`;
    switch (message.type) {
      case 'connected':
        this.connectionId = message.connection_id;
        break;
      case 'error': {
        this.errors.push(message);
        const waiter = this.waiting.get(key);
        if (waiter) {
          this.waiting.delete(key);
          waiter(message);
        }
        break;
      }
      case 'subscribed': {
        this.acks.set(key, message);
        this.snapshot(message);
        const waiter = this.waiting.get(key);
        if (waiter) {
          this.waiting.delete(key);
          waiter(message);
        }
        break;
      }
      case 'channel_batch_data':
        for (const contents of message.contents) this.update(message, contents);
        break;
      case 'channel_data':
        this.update(message, message.contents);
        break;
      default:
        break;
    }
  }

  snapshot(message) {
    const { channel, id, contents } = message;
    if (channel === 'v4_markets') {
      this.markets = structuredClone(contents.markets);
    } else if (channel === 'v4_orderbook') {
      const side = (levels) => Object.fromEntries(levels.map((l) => [l.price, l.size]));
      this.books[id] = { bids: side(contents.bids), asks: side(contents.asks) };
    } else if (channel === 'v4_trades') {
      this.trades[id] = [...contents.trades];
    } else if (channel === 'v4_candles') {
      this.candles[id] = Object.fromEntries(contents.candles.map((c) => [c.startedAt, c]));
    } else if (channel === 'v4_parent_subaccounts') {
      const account = { children: {}, orders: {}, fills: [], transfers: [], empty: true };
      if (contents && Object.keys(contents).length > 0) {
        account.empty = false;
        account.subaccount = contents.subaccount;
        for (const child of contents.subaccount.childSubaccounts) {
          account.children[child.subaccountNumber] = {
            assetPositions: { ...child.assetPositions },
            openPerpetualPositions: { ...child.openPerpetualPositions },
          };
        }
        for (const order of contents.orders) account.orders[order.id] = order;
      }
      this.accounts[id] = account;
      this.accountLog[id] = [];
    }
  }

  update(message, contents) {
    const { channel, id } = message;
    if (channel === 'v4_markets') {
      for (const [ticker, price] of Object.entries(contents.oraclePrices ?? {})) {
        if (this.markets[ticker]) this.markets[ticker].oraclePrice = price.oraclePrice;
      }
      for (const [ticker, fields] of Object.entries(contents.trading ?? {})) {
        this.markets[ticker] = { ...(this.markets[ticker] ?? {}), ...fields };
      }
    } else if (channel === 'v4_orderbook') {
      for (const side of ['bids', 'asks']) {
        for (const [price, size] of contents[side] ?? []) {
          if (fixed(size) === 0n) delete this.books[id][side][price];
          else this.books[id][side][price] = size;
        }
      }
    } else if (channel === 'v4_trades') {
      // Updates are oldest first; the list is kept newest first.
      this.trades[id].unshift(...[...contents.trades].reverse());
    } else if (channel === 'v4_candles') {
      this.candles[id][contents.startedAt] = contents;
    } else if (channel === 'v4_parent_subaccounts') {
      const account = this.accounts[id];
      this.accountLog[id].push({ subaccountNumber: message.subaccountNumber, contents });
      const child = (n) =>
        (account.children[n] ??= { assetPositions: {}, openPerpetualPositions: {} });
      for (const p of contents.assetPositions ?? []) {
        child(p.subaccountNumber).assetPositions[p.symbol] = p;
      }
      for (const p of contents.perpetualPositions ?? []) {
        const positions = child(p.subaccountNumber).openPerpetualPositions;
        positions[p.market] = { ...(positions[p.market] ?? {}), ...p };
        if (positions[p.market].status !== 'OPEN') delete positions[p.market];
      }
      for (const order of contents.orders ?? []) {
        account.orders[order.id] = { ...(account.orders[order.id] ?? {}), ...order };
      }
      for (const fill of contents.fills ?? []) account.fills.push(fill);
      if (contents.transfers) account.transfers.push(contents.transfers);
    }
  }

  /** A subaccount's state in a form two clients can be compared by. */
  accountView(id) {
    const account = this.accounts[id];
    const children = {};
    for (const [number, child] of Object.entries(account.children)) {
      const usdc = child.assetPositions.USDC;
      const positions = Object.fromEntries(
        Object.entries(child.openPerpetualPositions).map(([market, p]) => {
          // Unrealized profit moves with the mark price between two looks at a position.
          const { unrealizedPnl, ...rest } = p;
          return [market, rest];
        })
      );
      const balance = usdc && fixed(usdc.size) !== 0n ? { side: usdc.side, size: usdc.size } : null;
      if (balance || Object.keys(positions).length > 0) children[number] = { balance, positions };
    }
    const open = Object.values(account.orders).filter((o) => o.status === 'OPEN');
    return { children, openOrders: Object.fromEntries(open.map((o) => [o.id, o])) };
  }
}

// --------------------------------------------------------------------------------- contract

const DEC = /^-?\d+(\.\d+)?$/;
const ISO = /^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{3}Z$/;
const INT = /^\d+$/;

const dec = (v) => typeof v === 'string' && DEC.test(v);
const iso = (v) => typeof v === 'string' && ISO.test(v);
const int = (v) => typeof v === 'string' && INT.test(v);
const str = (v) => typeof v === 'string';
const num = (v) => typeof v === 'number' && Number.isFinite(v);
const bool = (v) => typeof v === 'boolean';
const oneOf = (...options) => (v) => options.includes(v);
const maybe = (type) => (v) => v === undefined || v === null || type(v);

// What the front end's validators require of each object.
const SHAPES = {
  market: {
    clobPairId: int,
    ticker: str,
    status: oneOf('ACTIVE', 'PAUSED', 'CANCEL_ONLY', 'POST_ONLY', 'INITIALIZING', 'FINAL_SETTLEMENT'),
    oraclePrice: dec,
    priceChange24H: dec,
    volume24H: dec,
    trades24H: num,
    nextFundingRate: dec,
    initialMarginFraction: dec,
    maintenanceMarginFraction: dec,
    openInterest: dec,
    atomicResolution: num,
    quantumConversionExponent: num,
    tickSize: dec,
    stepSize: dec,
    stepBaseQuantums: num,
    subticksPerTick: num,
    marketType: oneOf('CROSS', 'ISOLATED'),
    baseOpenInterest: dec,
  },
  order: {
    id: int,
    subaccountId: str,
    clientId: str,
    clobPairId: int,
    side: oneOf('BUY', 'SELL'),
    size: dec,
    totalFilled: dec,
    price: dec,
    type: oneOf('LIMIT', 'MARKET'),
    reduceOnly: bool,
    orderFlags: str,
    goodTilBlockTime: maybe(iso),
    createdAtHeight: int,
    clientMetadata: str,
    timeInForce: oneOf('GTT', 'FOK', 'IOC'),
    status: oneOf('OPEN', 'FILLED', 'CANCELED'),
    postOnly: bool,
    ticker: str,
    updatedAt: iso,
    updatedAtHeight: int,
    subaccountNumber: num,
  },
  fill: {
    id: str,
    side: oneOf('BUY', 'SELL'),
    liquidity: oneOf('TAKER', 'MAKER'),
    type: oneOf('LIMIT', 'LIQUIDATED', 'LIQUIDATION', 'DELEVERAGED', 'OFFSETTING'),
    market: str,
    marketType: oneOf('PERPETUAL'),
    price: dec,
    size: dec,
    fee: dec,
    affiliateRevShare: dec,
    createdAt: iso,
    createdAtHeight: int,
    orderId: maybe(int),
    subaccountNumber: num,
    positionSizeBefore: maybe(dec),
    entryPriceBefore: maybe(dec),
    positionSideBefore: maybe(oneOf('LONG', 'SHORT')),
  },
  trade: {
    id: str,
    side: oneOf('BUY', 'SELL'),
    size: dec,
    price: dec,
    type: oneOf('LIMIT', 'LIQUIDATED', 'DELEVERAGED'),
    createdAt: iso,
    createdAtHeight: int,
  },
  candle: {
    startedAt: iso,
    ticker: str,
    resolution: oneOf('1MIN', '5MINS', '15MINS', '30MINS', '1HOUR', '4HOURS', '1DAY'),
    low: dec,
    high: dec,
    open: dec,
    close: dec,
    baseTokenVolume: dec,
    usdVolume: dec,
    trades: num,
    startingOpenInterest: dec,
  },
  position: {
    market: str,
    status: oneOf('OPEN', 'CLOSED', 'LIQUIDATED'),
    side: oneOf('LONG', 'SHORT'),
    size: dec,
    maxSize: dec,
    entryPrice: dec,
    realizedPnl: dec,
    createdAt: iso,
    createdAtHeight: int,
    sumOpen: dec,
    sumClose: dec,
    netFunding: dec,
    unrealizedPnl: dec,
    closedAt: maybe(iso),
    exitPrice: maybe(dec),
    subaccountNumber: num,
  },
  asset: {
    symbol: str,
    side: oneOf('LONG', 'SHORT'),
    size: dec,
    assetId: str,
    subaccountNumber: num,
  },
  subaccount: {
    address: str,
    subaccountNumber: num,
    equity: dec,
    freeCollateral: dec,
    marginEnabled: bool,
    updatedAtHeight: int,
    latestProcessedBlockHeight: int,
  },
  transfer: {
    size: dec,
    createdAt: iso,
    createdAtHeight: int,
    symbol: str,
    type: oneOf('TRANSFER_IN', 'TRANSFER_OUT', 'DEPOSIT', 'WITHDRAWAL'),
    transactionHash: str,
  },
  fundingPayment: {
    createdAt: iso,
    createdAtHeight: int,
    perpetualId: int,
    ticker: str,
    oraclePrice: dec,
    size: dec,
    side: oneOf('LONG', 'SHORT'),
    rate: dec,
    payment: dec,
    subaccountNumber: int,
    fundingIndex: dec,
  },
  historicalFunding: {
    ticker: str,
    rate: dec,
    price: dec,
    effectiveAt: iso,
    effectiveAtHeight: int,
  },
  pnlTick: {
    equity: dec,
    netTransfers: dec,
    totalPnl: dec,
    createdAt: iso,
    createdAtHeight: int,
  },
  historicalPnlTick: {
    equity: dec,
    totalPnl: dec,
    netTransfers: dec,
    createdAt: iso,
    blockHeight: int,
    blockTime: iso,
  },
  tradeHistory: {
    id: str,
    marketId: str,
    // A string or absent. The front end's check of this object refuses null here.
    orderId: (v) => v === undefined || typeof v === 'string',
    side: oneOf('BUY', 'SELL'),
    positionSide: oneOf('LONG', 'SHORT'),
    entryPrice: dec,
    executionPrice: dec,
    value: dec,
    prevSize: dec,
    additionalSize: dec,
    netFee: dec,
    time: iso,
    action: oneOf('OPEN', 'CLOSE', 'PARTIAL_CLOSE', 'EXTEND', 'LIQUIDATION_CLOSE', 'LIQUIDATION_PARTIAL_CLOSE'),
    marginMode: oneOf('CROSS', 'ISOLATED'),
    netRealizedPnl: maybe(dec),
    subaccountNumber: num,
  },
};

const shapeCounts = {};
/** Checks objects against a shape, once per (label, shape), reporting the first violations. */
function conforms(label, shape, objects) {
  const bad = [];
  for (const object of objects) {
    for (const [field, type] of Object.entries(SHAPES[shape])) {
      if (!type(object?.[field])) bad.push(`${field}=${JSON.stringify(object?.[field])}`);
    }
  }
  shapeCounts[shape] = (shapeCounts[shape] ?? 0) + objects.length;
  check(`${label}: ${objects.length} ${shape} objects have the required fields`, bad.length === 0, bad.slice(0, 5).join(', '));
}

// ----------------------------------------------------------------------------------- checks

async function subscribeAll(stream) {
  const answers = [await stream.subscribe('v4_markets')];
  for (const ticker of TICKERS) {
    answers.push(await stream.subscribe('v4_orderbook', ticker));
    answers.push(await stream.subscribe('v4_trades', ticker));
    for (const resolution of RESOLUTIONS) {
      answers.push(await stream.subscribe('v4_candles', `${ticker}/${resolution}`));
    }
  }
  for (const parent of PARENTS) {
    answers.push(await stream.subscribe('v4_parent_subaccounts', `${OWNER}/${parent}`));
  }
  return answers;
}

function checkProtocol(stream, label) {
  check(`${label}: message IDs have no gaps over ${stream.messages} messages`, stream.gaps.length === 0, JSON.stringify(stream.gaps.slice(0, 5)));
  check(`${label}: no errors were sent`, stream.errors.length === 0, JSON.stringify(stream.errors.slice(0, 3)));
  check(`${label}: the connection stayed open`, !stream.closed, JSON.stringify(stream.closed));
}

/** Retries a comparison of two moving states until they agree or time runs out. */
async function converge(name, got, want, timeoutMs = 6000) {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    const [g, w] = [JSON.stringify(sorted(got())), JSON.stringify(sorted(want()))];
    if (g === w || Date.now() > deadline) {
      check(name, g === w, g === w ? '' : `\n  got  ${clip(g)}\n  want ${clip(w)}`);
      return;
    }
    await sleep(200);
  }
}

async function compareStreams(early, late) {
  await converge('stream: markets followed from the start equal a fresh subscription', () => early.markets, () => late.markets);
  for (const ticker of TICKERS) {
    await converge(`stream: ${ticker} book followed from the start equals a fresh subscription`, () => early.books[ticker], () => late.books[ticker]);
    const fresh = late.trades[ticker];
    same(`stream: ${ticker} tape followed from the start equals a fresh subscription`, early.trades[ticker].slice(0, fresh.length), fresh);
    for (const resolution of RESOLUTIONS) {
      const id = `${ticker}/${resolution}`;
      const strip = (candles) => Object.fromEntries(Object.entries(candles).map(([k, { id: _, ...c }]) => [k, c]));
      const freshCandles = strip(late.candles[id]);
      const followed = strip(early.candles[id]);
      same(`stream: ${id} candles followed from the start equal a fresh subscription`, Object.fromEntries(Object.keys(freshCandles).map((k) => [k, followed[k]])), freshCandles);
    }
  }
  for (const parent of PARENTS) {
    const id = `${OWNER}/${parent}`;
    await converge(`stream: account ${parent} followed from the start equals a fresh subscription`, () => early.accountView(id), () => late.accountView(id));
  }
}

async function checkProtocolEdges() {
  const stream = new Stream('edges');
  const connected = await stream.open();
  check('protocol: connected is the first message, with ID 0', connected.type === 'connected' && connected.message_id === 0 && typeof connected.connection_id === 'string');

  const ticker = TICKERS[0];
  const first = await stream.subscribe('v4_orderbook', ticker);
  check('protocol: subscribed echoes the channel and ID', first.type === 'subscribed' && first.channel === 'v4_orderbook' && first.id === ticker && first.connection_id === stream.connectionId);
  const again = await stream.subscribe('v4_orderbook', ticker);
  check('protocol: a second subscribe is refused with the text clients match', again.type === 'error' && again.message === `Invalid subscribe message: already subscribed (v4_orderbook-${ticker})`, again.message);

  const markets = await stream.subscribe('v4_markets');
  check('protocol: the markets channel has no ID', markets.type === 'subscribed' && !('id' in markets));
  const marketsAgain = await stream.subscribe('v4_markets');
  check('protocol: a second markets subscribe names the channel twice', marketsAgain.message === 'Invalid subscribe message: already subscribed (v4_markets-v4_markets)', marketsAgain.message);

  let answer = stream.next('unsubscribed');
  stream.send({ type: 'unsubscribe', channel: 'v4_orderbook', id: ticker });
  const unsubscribed = await answer;
  check('protocol: unsubscribe is acknowledged', unsubscribed.channel === 'v4_orderbook' && unsubscribed.id === ticker);
  const back = await stream.subscribe('v4_orderbook', ticker);
  check('protocol: a channel can be subscribed again after unsubscribing', back.type === 'subscribed');

  for (const [message, expected] of [
    [{ type: 'subscribe', channel: 'v4_orderbook', id: 'NOPE-USD' }, 'Invalid subscribe message: unknown market (v4_orderbook-NOPE-USD)'],
    [{ type: 'subscribe', channel: 'v4_candles', id: `${ticker}/2MIN` }, `Invalid subscribe message: invalid id (v4_candles-${ticker}/2MIN)`],
    [{ type: 'subscribe', channel: 'v4_parent_subaccounts', id: 'dydx1abc/0' }, 'Invalid subscribe message: invalid id (v4_parent_subaccounts-dydx1abc/0)'],
    [{ type: 'subscribe', channel: 'v4_nothing' }, 'Invalid channel: v4_nothing'],
    [{ type: 'dance' }, 'Invalid message type: dance'],
  ]) {
    answer = stream.next('error');
    stream.send(message);
    const error = await answer;
    check(`protocol: ${JSON.stringify(message)} is refused`, error.message === expected, error.message);
  }
  answer = stream.next('error');
  stream.socket.send('not json');
  check('protocol: an unparsable message is refused', (await answer).message === 'Invalid message: could not parse');

  answer = stream.next('pong');
  stream.send({ type: 'ping' });
  check('protocol: ping is answered with pong', (await answer).type === 'pong');

  // An address that has no account subscribes fine and holds nothing.
  const stranger = await stream.subscribe('v4_parent_subaccounts', `0x${'9'.repeat(64)}/0`);
  check('protocol: an address without an account is subscribed with empty contents', stranger.type === 'subscribed' && Object.keys(stranger.contents).length === 0, JSON.stringify(stranger).slice(0, 200));

  check('protocol: message IDs have no gaps across errors and acknowledgements', stream.gaps.length === 0, JSON.stringify(stream.gaps));
  stream.close();
}

async function checkPublicRest(late, probe) {
  const { height, time } = await rest('/v4/height');
  const [watermark] = sql("SELECT checkpoint_hi_inclusive AS checkpoint FROM watermarks WHERE pipeline = 'state'");
  check('rest: height follows the state watermark', int(height) && iso(time) && Math.abs(Number(height) - watermark.checkpoint) <= 50, `${height} vs ${watermark.checkpoint}`);
  const clock = await rest('/v4/time');
  check('rest: time has an ISO string and an epoch', iso(clock.iso) && num(clock.epoch));
  const health = await rest('/health');
  check('rest: the service reports itself healthy', health.status === 'ok', JSON.stringify(health));

  const { markets } = await rest('/v4/perpetualMarkets');
  conforms('rest markets', 'market', Object.values(markets));
  same('rest: markets are the listed tickers', Object.keys(markets).sort(), [...TICKERS].sort());
  await converge('rest: markets equal the markets channel', () => late.markets, () => markets, 0);

  for (const ticker of TICKERS) {
    const one = await rest(`/v4/perpetualMarkets?ticker=${ticker}`);
    same(`rest: ${ticker} alone is the same market`, Object.keys(one.markets), [ticker]);

    const [market] = sql(`SELECT market, market_index FROM markets WHERE market = '${deployment.markets[ticker].clearingHouse}'`);
    const id = market.market;
    check(`rest: ${ticker} is numbered by the indexer`, markets[ticker].clobPairId === String(market.market_index));

    // The book against the open orders it is made of.
    const book = await rest(`/v4/orderbooks/perpetualMarket/${ticker}`);
    const levels = sql(`SELECT is_ask, trim_scale(price)::text AS price, trim_scale(SUM(remaining))::text AS size FROM orders WHERE market = '${id}' AND status = 'open' GROUP BY is_ask, price ORDER BY price`);
    const side = (isAsk) => levels.filter((l) => l.is_ask === isAsk).map(({ price, size }) => ({ price, size }));
    same(`rest: ${ticker} asks are the open asks, best first`, book.asks, side(true));
    same(`rest: ${ticker} bids are the open bids, best first`, book.bids, side(false).reverse());
    const flat = (levels) => Object.fromEntries(levels.map((l) => [l.price, l.size]));
    same(`stream: ${ticker} book equals the open orders`, late.books[ticker], { bids: flat(book.bids), asks: flat(book.asks) });

    // The tape against the maker fills. The side is the taker's: it bought what a maker sold.
    const { trades } = await rest(`/v4/trades/perpetualMarket/${ticker}`);
    conforms(`rest ${ticker} trades`, 'trade', trades);
    const fills = sql(`SELECT checkpoint || '-' || tx_index || '-' || event_index || '-' || fill_index AS id, is_ask AS taker_bought, trim_scale(price)::text AS price, trim_scale(size)::text AS size FROM fills WHERE market = '${id}' AND kind = 'trade' AND liquidity = 'maker' ORDER BY checkpoint DESC, tx_index DESC, event_index DESC, fill_index DESC LIMIT 1000`);
    same(`rest: ${ticker} trades are the maker fills, newest first`, trades.map((t) => [t.id, t.side, t.price, t.size]), fills.map((f) => [f.id, f.taker_bought ? 'BUY' : 'SELL', f.price, f.size]));
    same(`stream: ${ticker} tape equals the latest trades`, late.trades[ticker], trades.slice(0, late.trades[ticker].length));
    const paged = await rest(`/v4/trades/perpetualMarket/${ticker}?limit=2&page=2`);
    same(`rest: ${ticker} trades page 2 of 2 is rows 3 and 4`, paged.trades, trades.slice(2, 4));

    // Candles against the table, and against the trades they summarize.
    for (const [resolution, ms] of [['1MIN', 60000], ['1HOUR', 3600000], ['1DAY', 86400000]]) {
      const { candles } = await rest(`/v4/candles/perpetualMarkets/${ticker}?resolution=${resolution}`);
      conforms(`rest ${ticker} ${resolution} candles`, 'candle', candles);
      const rows = sql(`SELECT start_ms, trim_scale(open)::text AS open, trim_scale(high)::text AS high, trim_scale(low)::text AS low, trim_scale(close)::text AS close, trim_scale(base_volume)::text AS base, trim_scale(quote_volume)::text AS quote, trades FROM candles WHERE market = '${id}' AND resolution_ms = ${ms} ORDER BY start_ms DESC`);
      same(`rest: ${ticker} ${resolution} candles are the table's, newest first`, candles.map((c) => [Date.parse(c.startedAt), c.open, c.high, c.low, c.close, c.baseTokenVolume, c.usdVolume, c.trades]), rows.map((r) => [r.start_ms, r.open, r.high, r.low, r.close, r.base, r.quote, r.trades]));
      check(`rest: ${ticker} ${resolution} candles count every trade once`, candles.reduce((n, c) => n + c.trades, 0) === fills.length, `${candles.reduce((n, c) => n + c.trades, 0)} vs ${fills.length}`);
      if (candles.length > 0) {
        // toISO is exclusive: the chart asks for what comes before the oldest candle it has.
        const justAfter = new Date(Date.parse(candles.at(-1).startedAt) + 1).toISOString();
        const before = await rest(`/v4/candles/perpetualMarkets/${ticker}?resolution=${resolution}&toISO=${justAfter}&limit=1`);
        same(`rest: ${ticker} ${resolution} candles before a time are the ones that started before it`, before.candles, [candles.at(-1)]);
        const none = await rest(`/v4/candles/perpetualMarkets/${ticker}?resolution=${resolution}&toISO=${candles.at(-1).startedAt}`);
        same(`rest: ${ticker} ${resolution} there are no candles before the oldest`, none.candles, []);
        const from = await rest(`/v4/candles/perpetualMarkets/${ticker}?resolution=${resolution}&fromISO=${candles.at(-1).startedAt}&limit=1000`);
        same(`rest: ${ticker} ${resolution} candles from a time include the one at it`, from.candles.at(-1), candles.at(-1));
      }
    }

    const funding = await rest(`/v4/historicalFunding/${ticker}`);
    conforms(`rest ${ticker} historical funding`, 'historicalFunding', funding.historicalFunding);
    const [updates] = sql(`SELECT COUNT(*) AS count, COUNT(index_price) AS priced FROM funding_updates WHERE market = '${id}'`);
    check(`rest: ${ticker} funding history has every update`, funding.historicalFunding.length === Math.min(updates.count, 100), `${funding.historicalFunding.length} vs ${updates.count}`);
    check(`rest: ${ticker} funding updates all carry an index price`, updates.priced === updates.count, `${updates.priced} of ${updates.count}`);
    check(`rest: ${ticker} funding rates are small fractions of a real price`, funding.historicalFunding.every((f) => near(f.rate, '0', '0.05') && fixed(f.price) > 0n), JSON.stringify(funding.historicalFunding.slice(0, 2)));
    if (probe) {
      // The scenario makes longs pay once: that update shows as a positive rate, and the
      // market's next rate still leans the same way.
      check(`rest: ${ticker} funding history shows the interval that charged the longs`, funding.historicalFunding.some((f) => fixed(f.rate) > 0n), JSON.stringify(funding.historicalFunding));
    }
  }

  for (const period of ['ONE_DAY', 'SEVEN_DAYS']) {
    const lines = await rest(`/v4/sparklines?timePeriod=${period}`);
    check(`rest: ${period} sparklines list every market`, TICKERS.every((t) => Array.isArray(lines[t]) && lines[t].every(dec)), JSON.stringify(lines).slice(0, 200));
  }

  for (const [path, status, text] of [
    ['/v4/orderbooks/perpetualMarket/NOPE-USD', 404, 'NOPE-USD not found'],
    [`/v4/candles/perpetualMarkets/${TICKERS[0]}?resolution=2MIN`, 400, 'resolution must be'],
    [`/v4/trades/perpetualMarket/${TICKERS[0]}?limit=0`, 400, 'limit must be'],
    ['/v4/fills/parentSubaccountNumber?parentSubaccountNumber=0', 400, 'address is required'],
    [`/v4/fills/parentSubaccountNumber?address=${OWNER}&parentSubaccountNumber=128`, 400, 'parentSubaccountNumber must be'],
    [`/v4/transfers/parentSubaccountNumber?address=0x${'9'.repeat(64)}&parentSubaccountNumber=0`, 404, 'No subaccount found with address'],
    [`/v4/addresses/0x${'9'.repeat(64)}/parentSubaccountNumber/0`, 404, 'No subaccount found with address'],
    [`/v4/pnl/parentSubaccountNumber?address=0x${'9'.repeat(64)}&parentSubaccountNumber=0`, 404, 'No subaccount found with address'],
    [`/v4/pnl/parentSubaccountNumber?address=${OWNER}&parentSubaccountNumber=0&daily=maybe`, 400, 'daily must be'],
    // Skipping rows costs the database as much as returning them: a request may not page deep.
    [`/v4/trades/perpetualMarket/${TICKERS[0]}?limit=1000&page=27`, 400, 'exceeds the maximum'],
    [`/v4/fills/parentSubaccountNumber?address=${OWNER}&parentSubaccountNumber=0&limit=100&page=252`, 400, 'exceeds the maximum'],
  ]) {
    const body = await rest(path, status);
    check(`rest: ${path.slice(0, 60)} answers ${status} in the API's error shape`, body.errors?.[0]?.msg?.includes(text), JSON.stringify(body));
  }
  const nobody = `address=0x${'9'.repeat(64)}&parentSubaccountNumber=0`;
  same('rest: an address without an account has no fills', await rest(`/v4/fills/parentSubaccountNumber?${nobody}`), { fills: [] });
  same('rest: an address without an account has no orders', await rest(`/v4/orders/parentSubaccountNumber?${nobody}`), []);
  same('rest: unkept histories answer empty', await rest(`/v4/historicalBlockTradingRewards/${OWNER}`), { rewards: [] });
  same('rest: the deepest page allowed is still answered', (await rest(`/v4/trades/perpetualMarket/${TICKERS[0]}?limit=1000&page=26`)).trades, []);

  // How long each kind of response may be reused by a cache in front of the API.
  const cached = async (path) => (await fetch(`${API}${path}`)).headers.get('cache-control');
  same('rest: live data may be cached for a second, slow history for ten, the clock never', [
    await cached('/v4/perpetualMarkets'),
    await cached(`/v4/orderbooks/perpetualMarket/${TICKERS[0]}`),
    await cached(`/v4/historicalFunding/${TICKERS[0]}`),
    await cached('/v4/sparklines'),
    await cached('/v4/time'),
  ], ['public, max-age=1', 'public, max-age=1', 'public, max-age=10', 'public, max-age=10', 'no-cache, no-store, no-transform']);
  same('rest: errors, the height and health are not cached', [await cached('/v4/orderbooks/perpetualMarket/NOPE-USD'), await cached('/v4/height'), await cached('/health')], [null, null, null]);
  check('rest: compliance screening answers', (await rest(`/v4/compliance/screen/${OWNER}`)).status === 'COMPLIANT');
}

async function checkAccounts(early, late, probe) {
  const accounts = sql(`SELECT a.account_id, a.collateral FROM account_caps c JOIN accounts a ON a.object_id = c.account_object_id WHERE c.owner = '${OWNER}' AND c.role = 'admin' ORDER BY a.account_id`);
  check('accounts: the owner holds the capabilities the scenario created', accounts.length === PARENTS.length, `${accounts.length} accounts`);

  for (const parent of PARENTS) {
    const account = accounts[parent];
    if (!account) continue;
    const accountId = account.account_id;
    const query = `address=${OWNER}&parentSubaccountNumber=${parent}`;
    const id = `${OWNER}/${parent}`;
    const label = `account ${parent} (#${accountId})`;
    const numberOf = (ticker) => parent + 128 * (Number(late.markets[ticker].clobPairId) + 1);

    // The subaccount: REST against the stream, and both against the chain.
    const { subaccount } = await rest(`/v4/addresses/${OWNER}/parentSubaccountNumber/${parent}`);
    conforms(`${label} subaccounts`, 'subaccount', subaccount.childSubaccounts);
    const positions = subaccount.childSubaccounts.flatMap((c) => Object.values(c.openPerpetualPositions));
    const assets = subaccount.childSubaccounts.flatMap((c) => Object.values(c.assetPositions));
    conforms(`${label} positions`, 'position', positions);
    conforms(`${label} asset positions`, 'asset', assets);
    check(`${label}: the parent subaccount comes first and children follow its number`, subaccount.childSubaccounts[0].subaccountNumber === parent && subaccount.childSubaccounts.slice(1).every((c) => c.subaccountNumber % 128 === parent && c.subaccountNumber >= 128));
    const restView = {
      children: Object.fromEntries(
        subaccount.childSubaccounts
          .map((c) => {
            const usdc = c.assetPositions.USDC;
            const open = Object.fromEntries(Object.entries(c.openPerpetualPositions).map(([m, { unrealizedPnl, ...p }]) => [m, p]));
            return [c.subaccountNumber, { balance: usdc ? { side: usdc.side, size: usdc.size } : null, positions: open }];
          })
          .filter(([, c]) => c.balance || Object.keys(c.positions).length > 0)
      ),
    };
    same(`${label}: the subaccount over REST equals the stream's`, restView.children, late.accountView(id).children);
    for (const p of positions) {
      check(`${label}: ${p.market} position totals add up to its size`, fixed(p.sumOpen) - fixed(p.sumClose) === abs(fixed(p.size)) && fixed(p.maxSize) >= abs(fixed(p.size)), JSON.stringify(p));
    }

    if (probe) {
      const chain = probe.accounts[String(accountId)];
      const parentBalance = subaccount.childSubaccounts[0].assetPositions.USDC?.size ?? '0';
      check(`${label}: the unallocated balance equals the account object's`, fixed(parentBalance) === BigInt(chain.balance) * 10n ** BigInt(18 - DECIMALS), `${parentBalance} vs ${chain.balance}`);
      for (const ticker of TICKERS) {
        const child = subaccount.childSubaccounts.find((c) => c.subaccountNumber === numberOf(ticker));
        const usdc = child?.assetPositions.USDC;
        const balance = usdc ? (usdc.side === 'LONG' ? fixed(usdc.size) : -fixed(usdc.size)) : 0n;
        // Collateral is worth 1 in the scenario, so the balance is collateral plus accrued
        // funding minus what the position cost.
        const expected = BigInt(chain.collateral) + BigInt(chain.unsettled_funding) - BigInt(chain.quote);
        check(`${label}: ${ticker} quote balance equals the engine's collateral, funding and entry notional`, abs(balance - expected) <= 10n, `${balance} vs ${expected}`);
        const position = child?.openPerpetualPositions[ticker];
        check(`${label}: ${ticker} position size equals the engine's`, fixed(position?.size ?? '0') === BigInt(chain.base), `${position?.size} vs ${chain.base}`);
        if (position) {
          // Valued at the mark price, the child's equity is the engine's margin.
          const mark = BigInt(probe.mark_price);
          const margin = expected + (BigInt(chain.base) * mark) / 10n ** 18n;
          const equity = fixed(child.equity);
          const want = margin > 0n ? margin : 0n;
          check(`${label}: ${ticker} equity equals the engine's margin at the mark price`, abs(equity - want) <= abs(want) / 10000n + 10n ** 12n, `${equity} vs ${want}`);
        }
      }
    }

    // Orders against the table.
    const orders = await rest(`/v4/orders/parentSubaccountNumber?${query}&returnLatestOrders=true`);
    conforms(`${label} orders`, 'order', orders);
    const orderRows = sql(`SELECT order_id::text AS id, status, is_ask, trim_scale(price)::text AS price, trim_scale(size)::text AS size, trim_scale(filled)::text AS filled FROM orders WHERE account_id = ${accountId}`);
    const byId = (list) => Object.fromEntries(list.map((o) => [o[0], o]));
    same(`${label}: orders over REST are the table's`, byId(orders.map((o) => [o.id, o.status, o.side, o.price, o.size, o.totalFilled])), byId(orderRows.map((o) => [o.id, o.status.toUpperCase(), o.is_ask ? 'SELL' : 'BUY', o.price, o.size, o.filled])));
    const open = await rest(`/v4/orders/parentSubaccountNumber?${query}&status=OPEN`);
    same(`${label}: open orders over REST equal the stream's`, Object.fromEntries(open.map((o) => [o.id, o])), late.accountView(id).openOrders);
    check(`${label}: every order sits in its market's child subaccount`, orders.every((o) => o.subaccountNumber === numberOf(o.ticker)));
    // Every order the early client was told about ended in the state the table holds.
    const streamed = Object.values(early.accounts[id].orders);
    same(`${label}: orders followed over the stream ended as the table has them`, byId(streamed.map((o) => [o.id, o.status, o.totalFilled])), byId(orderRows.map((o) => [o.id, o.status.toUpperCase(), o.filled])));

    // Fills against the table, over REST and as they were pushed.
    const { fills } = await rest(`/v4/fills/parentSubaccountNumber?${query}`);
    conforms(`${label} fills`, 'fill', fills);
    const fillRows = sql(`SELECT checkpoint || '-' || tx_index || '-' || event_index || '-' || fill_index AS id, is_ask, kind, liquidity, trim_scale(size)::text AS size, trim_scale(fee)::text AS fee FROM fills WHERE account_id = ${accountId} ORDER BY checkpoint DESC, tx_index DESC, event_index DESC, fill_index DESC`);
    same(`${label}: fills over REST are the table's, newest first`, fills.map((f) => [f.id, f.side, f.liquidity, f.size, f.fee]), fillRows.map((f) => [f.id, f.is_ask ? 'SELL' : 'BUY', f.liquidity.toUpperCase(), f.size, f.fee]));
    const pushed = early.accounts[id].fills;
    conforms(`${label} pushed fills`, 'fill', pushed);
    same(`${label}: every fill was pushed exactly once, in chain order`, pushed.map((f) => f.id), fillRows.map((f) => f.id).reverse());
    if (fills.length >= 4) {
      const page = await rest(`/v4/fills/parentSubaccountNumber?${query}&limit=2&page=2`);
      same(`${label}: fills page 2 of 2 is rows 3 and 4, with totals`, [page.fills, page.pageSize, page.offset, page.totalResults], [fills.slice(2, 4), 2, 2, fills.length]);
    }

    // Trade history: one entry per fill, and the actions replay the position.
    const { tradeHistory } = await rest(`/v4/tradeHistory/parentSubaccountNumber?${query}`);
    conforms(`${label} trade history`, 'tradeHistory', tradeHistory);
    same(`${label}: trade history has an entry per fill`, tradeHistory.map((t) => t.id), fills.map((f) => f.id));
    const sizes = new Map();
    let consistent = true;
    for (const entry of [...tradeHistory].reverse()) {
      const size = sizes.get(entry.marketId) ?? 0n;
      const before = abs(size);
      consistent &&= fixed(entry.prevSize) === before;
      const delta = entry.side === 'BUY' ? fixed(entry.additionalSize) : -fixed(entry.additionalSize);
      const grows = size === 0n || size > 0n === delta > 0n;
      const expected = size === 0n ? ['OPEN'] : grows ? ['EXTEND'] : abs(delta) < before ? ['PARTIAL_CLOSE', 'LIQUIDATION_PARTIAL_CLOSE'] : ['CLOSE', 'LIQUIDATION_CLOSE'];
      consistent &&= expected.includes(entry.action);
      consistent &&= grows === (entry.netRealizedPnl == null);
      sizes.set(entry.marketId, size + delta);
    }
    const replayed = Object.fromEntries([...sizes].filter(([, n]) => n !== 0n).map(([m, n]) => [m, n.toString()]));
    const held = Object.fromEntries(positions.map((p) => [p.market, fixed(p.size).toString()]));
    check(`${label}: trade history actions replay each position to its size`, consistent && JSON.stringify(sorted(replayed)) === JSON.stringify(sorted(held)), `replayed ${JSON.stringify(replayed)}, held ${JSON.stringify(held)}`);

    // Transfers against the table.
    const { transfers } = await rest(`/v4/transfers/parentSubaccountNumber?${query}`);
    conforms(`${label} transfers`, 'transfer', transfers);
    const transferRows = sql(`SELECT kind, amount::text AS amount, tx_digest FROM collateral_transfers WHERE account_id = ${accountId} AND kind IN ('deposit', 'withdraw') ORDER BY checkpoint DESC, tx_index DESC, event_index DESC`);
    same(`${label}: transfers over REST are the deposits and withdrawals`, transfers.map((t) => [t.type, fixed(t.size).toString(), t.transactionHash]), transferRows.map((t) => [t.kind === 'deposit' ? 'DEPOSIT' : 'WITHDRAWAL', (BigInt(t.amount) * 10n ** BigInt(18 - DECIMALS)).toString(), t.tx_digest]));
    check(`${label}: a deposit comes from the wallet into the parent subaccount`, transfers.filter((t) => t.type === 'DEPOSIT').every((t) => t.sender.address === OWNER && t.sender.subaccountNumber === undefined && t.recipient.subaccountNumber === parent));
    // The account was created after the early client subscribed, so its deposit came with the
    // account's first state rather than as a transfer of its own.
    check(`${label}: no transfer was pushed twice`, new Set(early.accounts[id].transfers.map((t) => t.transactionHash)).size === early.accounts[id].transfers.length);

    // Funding payments against the table.
    const { fundingPayments } = await rest(`/v4/fundingPayments/parentSubaccount?${query}`);
    conforms(`${label} funding payments`, 'fundingPayment', fundingPayments);
    const paymentRows = sql(`SELECT trim_scale(collateral_change_usd)::text AS payment, trim_scale(ABS(position_base))::text AS size FROM funding_payments WHERE account_id = ${accountId} AND collateral_change_usd <> 0 ORDER BY checkpoint DESC, tx_index DESC, event_index DESC`);
    same(`${label}: funding payments over REST are the table's non-zero settlements`, fundingPayments.map((p) => [p.payment, p.size]), paymentRows.map((p) => [p.payment, p.size]));
    check(`${label}: funding payments are fractions of a position that was held`, fundingPayments.every((p) => fixed(p.size) > 0n && fixed(p.oraclePrice) > 0n && fixed(p.payment) < 0n === fixed(p.rate) > 0n), JSON.stringify(fundingPayments.slice(0, 2)));
    for (const p of positions) {
      // Funding settled since the position opened, plus what the engine says has accrued since.
      const market = deployment.markets[p.market].clearingHouse;
      const [{ settled }] = sql(`SELECT trim_scale(COALESCE(SUM(f.collateral_change_usd), 0))::text AS settled FROM positions p LEFT JOIN funding_payments f ON f.account_id = p.account_id AND f.market = p.market AND f.checkpoint >= p.opened_checkpoint WHERE p.account_id = ${accountId} AND p.market = '${market}'`);
      if (probe) {
        const accrued = BigInt(probe.accounts[String(accountId)].unsettled_funding);
        check(`${label}: ${p.market} net funding is what was settled plus what the engine says accrued`, abs(fixed(p.netFunding) - fixed(settled) - accrued) <= 10n, `${p.netFunding} vs ${settled} + ${accrued}`);
      }
    }

    // The PnL history against the table. Ticks keep being taken while this runs, so the table
    // is read first and the response is compared up to the newest tick the table had.
    const tickRows = sql(`SELECT bucket_ms, checkpoint::text AS height, trim_scale(equity)::text AS equity, trim_scale(net_transfers)::text AS net_transfers, trim_scale(total_pnl)::text AS total_pnl FROM pnl_ticks WHERE account_id = ${accountId} ORDER BY bucket_ms DESC LIMIT 1000`);
    const { pnl } = await rest(`/v4/pnl/parentSubaccountNumber?${query}`);
    conforms(`${label} pnl ticks`, 'pnlTick', pnl);
    check(`${label}: ticks of its PnL history were taken`, tickRows.length >= 3, `${tickRows.length} ticks`);
    const at = (tick) => new Date(tick.createdAt).getTime();
    if (tickRows.length >= 3) {
      const known = pnl.filter((t) => at(t) <= Number(tickRows[0].bucket_ms));
      same(`${label}: PnL ticks over REST are the table's, newest first`, known.map((t) => [at(t), t.createdAtHeight, t.equity, t.netTransfers, t.totalPnl]), tickRows.map((t) => [Number(t.bucket_ms), t.height, t.equity, t.net_transfers, t.total_pnl]));
      check(`${label}: every tick's PnL is its equity less what was transferred in`, pnl.every((t) => fixed(t.totalPnl) === fixed(t.equity) - fixed(t.netTransfers)));
      // Collateral is worth 1 in the scenario, so net transfers in USD are the coins moved.
      const [{ net }] = sql(`SELECT COALESCE(SUM(CASE kind WHEN 'deposit' THEN amount ELSE -amount END), 0)::text AS net FROM collateral_transfers WHERE account_id = ${accountId} AND kind IN ('deposit', 'withdraw')`);
      check(`${label}: the latest tick's net transfers are its deposits less its withdrawals`, fixed(pnl[0].netTransfers) === BigInt(net) * 10n ** BigInt(18 - DECIMALS), `${pnl[0].netTransfers} vs ${net}`);
      // Nothing has traded since the scenario ended, so the latest tick valued the account as
      // it is now, but for the mark price's funding component, which decays with time.
      const now = fixed((await rest(`/v4/addresses/${OWNER}/parentSubaccountNumber/${parent}`)).subaccount.equity);
      const ticked = fixed((await rest(`/v4/pnl/parentSubaccountNumber?${query}&limit=1`)).pnl[0].equity);
      check(`${label}: the latest tick's equity is the account's equity`, abs(ticked - now) <= abs(now) / 1000n + 10n ** 9n, `${ticked} vs ${now}`);

      const older = await rest(`/v4/pnl/parentSubaccountNumber?${query}&limit=2&createdBeforeOrAt=${new Date(at(known[1]) - 1000).toISOString()}`);
      same(`${label}: PnL ticks before a time are the ones that follow it`, older.pnl, known.slice(2, 4));
      const newer = await rest(`/v4/pnl/parentSubaccountNumber?${query}&createdOnOrAfterHeight=${known[1].createdAtHeight}&createdBeforeOrAtHeight=${known[0].createdAtHeight}`);
      same(`${label}: PnL ticks between two heights are those ticks`, newer.pnl.filter((t) => at(t) <= at(known[0])), known.slice(0, 2));

      const daily = (await rest(`/v4/pnl/parentSubaccountNumber?${query}&daily=true`)).pnl;
      const day = (ms) => Math.floor(ms / 86_400_000) * 86_400_000;
      const firstOfDay = new Map();
      for (const tick of pnl) firstOfDay.set(day(at(tick)), tick);
      same(`${label}: the daily PnL history is each day's first tick, at the day's start`, daily.map((t) => [at(t), t.createdAtHeight, t.equity, t.totalPnl]), [...firstOfDay].map(([start, t]) => [start, t.createdAtHeight, t.equity, t.totalPnl]));

      const { historicalPnl } = await rest(`/v4/historical-pnl/parentSubaccountNumber?${query}&limit=5`);
      conforms(`${label} historical pnl ticks`, 'historicalPnlTick', historicalPnl);
      check(`${label}: the older PnL endpoint serves the same ticks`, historicalPnl.length === Math.min(5, pnl.length) && historicalPnl.every((t) => at(t) <= new Date(t.blockTime).getTime()));
    }

    // What the early client was told, and when.
    const log = early.accountLog[id];
    check(`${label}: an account created after subscribing was announced empty, then filled in by updates`, early.accounts[id].empty && log.length > 0);
    check(`${label}: every update names the subaccount its contents belong to`, log.every((m) => num(m.subaccountNumber) && [...(m.contents.assetPositions ?? []), ...(m.contents.perpetualPositions ?? [])].every((p) => p.subaccountNumber === m.subaccountNumber) && int(m.contents.blockHeight)));
  }
}

async function checkEngine(late, probe) {
  // An account nothing touched after funding moved: its balance changed without any event of
  // its own, and the checks above compared it with the engine's.
  const accrued = Object.values(probe.accounts).filter((a) => BigInt(a.unsettled_funding) !== 0n && BigInt(a.base) !== 0n);
  check('engine: some position has funding accrued but not settled', accrued.length > 0, JSON.stringify(probe.accounts));
  for (const ticker of TICKERS) {
    const market = late.markets[ticker];
    const mark = BigInt(probe.mark_price);
    // The engine's mark price is taken a moment after the API's, and the funding component
    // decays with time: they agree to well within a basis point.
    check(`engine: ${ticker} mark price equals the engine's`, abs(fixed(market.oraclePrice) - mark) <= mark / 10000n, `${market.oraclePrice} vs ${probe.mark_price}`);
    const [row] = sql(`SELECT trim_scale(open_interest)::text AS oi, paused, closed FROM markets WHERE market = '${deployment.markets[ticker].clearingHouse}'`);
    check(`engine: ${ticker} open interest is the market object's`, market.openInterest === row.oi, `${market.openInterest} vs ${row.oi}`);
    check(`engine: ${ticker} status follows the market's pause mode`, market.status === (row.closed ? 'FINAL_SETTLEMENT' : ['ACTIVE', 'PAUSED', 'CANCEL_ONLY'][row.paused]));
  }
}

// ------------------------------------------------------------------------------------- main

const early = new Stream('early');
await early.open();
const first = await subscribeAll(early);
check('early: every subscription was acknowledged', first.every((m) => m.type === 'subscribed'), JSON.stringify(first.filter((m) => m.type !== 'subscribed')));
if (args.ready) writeFileSync(args.ready, `${early.connectionId}\n`);

if (args.stop) {
  console.log(`following the stream until ${args.stop} appears`);
  while (!existsSync(args.stop)) await sleep(250);
}
await early.quiet(1500);

const late = new Stream('late');
await late.open();
const second = await subscribeAll(late);
check('late: every subscription was acknowledged', second.every((m) => m.type === 'subscribed'));

conforms('stream markets', 'market', Object.values(late.markets));
for (const ticker of TICKERS) {
  conforms(`stream ${ticker} tape`, 'trade', late.trades[ticker]);
  conforms(`stream ${ticker} pushed trades`, 'trade', early.trades[ticker]);
  for (const resolution of RESOLUTIONS) {
    conforms(`stream ${ticker}/${resolution} candles`, 'candle', Object.values(late.candles[`${ticker}/${resolution}`]));
  }
}
for (const parent of PARENTS) {
  const account = late.accounts[`${OWNER}/${parent}`];
  if (account.empty) continue;
  conforms(`stream account ${parent} open orders`, 'order', Object.values(account.orders));
  conforms(`stream account ${parent} pushed orders`, 'order', Object.values(early.accounts[`${OWNER}/${parent}`].orders));
}

await compareStreams(early, late);
const probe = args.probe ? JSON.parse(readFileSync(args.probe, 'utf8')) : null;
await checkPublicRest(late, probe);
await checkAccounts(early, late, probe);
if (probe) await checkEngine(late, probe);
await checkProtocolEdges();
checkProtocol(early, 'early');
checkProtocol(late, 'late');
early.close();
late.close();

const failed = results.filter(([, ok]) => !ok);
console.log(`\nmessages: early ${early.messages}, late ${late.messages}; objects checked: ${JSON.stringify(shapeCounts)}`);
console.log(`${results.length - failed.length}/${results.length} checks passed`);
process.exit(failed.length === 0 ? 0 : 1);
