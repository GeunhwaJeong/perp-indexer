#!/usr/bin/env node
// Copyright (c) 2026 Geunhwa Jeong
// SPDX-License-Identifier: Apache-2.0

// Checks how the API behaves when things go wrong, against a stack that is already up (a local
// network being indexed, as `run_all.sh` leaves it with HOLD set).
//
//   scripts/localnet/faults.mjs --api-binary target/debug/perp-api \
//       --database-url postgres://... --deployment <file> [--api http://127.0.0.1:3002]
//
// It starts an API of its own with tight limits and checks that:
//
//   - a client that sends too many messages is disconnected;
//   - a peer that leaves the server's pings unanswered is given up on, and one that answers
//     them is not;
//   - a client that stops reading is disconnected instead of growing a queue in the server;
//   - when the feed falls further behind than it bridges with updates, every client is made to
//     start over, and can;
//   - when the database drops the API's connections, the service recovers by itself and a
//     client that stayed connected sees no gap (checked against the API at `--api`).
//
// `--tick` is a shell command that makes the chain change something, with `%PRICE%` where a
// new index price goes, e.g.
//
//   --tick 'HANEUL=<binary> HANEUL_CONFIG_DIR=<work dir>/config \
//           scripts/localnet/scenario.py price --perp-dex <engine copy> --usd %PRICE%'
//
// With it, recovery is checked by an update reaching the client; without it, by the client
// still being answered and the served height moving.
//
// Needs Node 22 or later and psql.

import { execFileSync, spawn } from 'node:child_process';
import { randomBytes } from 'node:crypto';
import { openSync, readFileSync } from 'node:fs';
import net from 'node:net';
import { parseArgs } from 'node:util';

const { values: args } = parseArgs({
  options: {
    'api-binary': { type: 'string' },
    'database-url': { type: 'string' },
    deployment: { type: 'string' },
    api: { type: 'string', default: 'http://127.0.0.1:3002' },
    log: { type: 'string', default: '/dev/null' },
    tick: { type: 'string' },
  },
});
if (!args['api-binary'] || !args['database-url'] || !args.deployment) {
  console.error('usage: faults.mjs --api-binary <path> --database-url <url> --deployment <file>');
  process.exit(2);
}

const PORT = 3003;
const METRICS_PORT = 9186;
const OWN = `http://127.0.0.1:${PORT}`;
const TICKER = Object.keys(JSON.parse(readFileSync(args.deployment, 'utf8')).markets)[0];
// The feed of the API under test bridges at most this many checkpoints with updates: more than
// the indexer commits at once, fewer than pass while the process is frozen below.
const MAX_ROUND_CHECKPOINTS = 20;
const FREEZE_MS = 10_000;
const PING_INTERVAL_S = 3;
const PONG_TIMEOUT_S = 2;

/** A masked WebSocket frame, as a client sends them: text by default, or a control frame. */
function frame(text, opcode = 0x1) {
  const payload = Buffer.from(text);
  const mask = randomBytes(4);
  const header = payload.length < 126 ? Buffer.from([0x80 | opcode, 0x80 | payload.length]) : Buffer.from([0x80 | opcode, 0x80 | 126, payload.length >> 8, payload.length & 0xff]);
  return Buffer.concat([header, mask, payload.map((byte, i) => byte ^ mask[i % 4])]);
}
const PONG = 0xa;

/** A raw socket upgraded to a WebSocket, so that nothing answers or reads on the client's behalf. */
async function rawClient() {
  const socket = net.connect(PORT, '127.0.0.1');
  await new Promise((resolve) => socket.once('connect', resolve));
  socket.write(`GET /v4/ws HTTP/1.1\r\nHost: 127.0.0.1:${PORT}\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: ${randomBytes(16).toString('base64')}\r\nSec-WebSocket-Version: 13\r\n\r\n`);
  await new Promise((resolve) => socket.once('data', resolve));
  return socket;
}

const results = [];
function check(name, ok, detail = '') {
  results.push([name, Boolean(ok)]);
  console.log(`${ok ? 'ok  ' : 'FAIL'} ${name}${ok || !detail ? '' : `: ${detail}`}`);
}
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function until(condition, timeoutMs, stepMs = 100) {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    const value = await condition();
    if (value || Date.now() > deadline) return value;
    await sleep(stepMs);
  }
}

/** The API's metrics as `name{labels}` -> value. */
async function metrics(port = METRICS_PORT) {
  const text = await (await fetch(`http://127.0.0.1:${port}/metrics`)).text();
  const values = {};
  for (const line of text.split('\n')) {
    if (line.startsWith('#') || !line.trim()) continue;
    const at = line.lastIndexOf(' ');
    values[line.slice(0, at)] = Number(line.slice(at + 1));
  }
  return values;
}

let price = 30_000;
/** Makes the chain move the market's index price, if the caller said how. */
function tick() {
  if (!args.tick) return false;
  price += 10;
  execFileSync('sh', ['-c', args.tick.replaceAll('%PRICE%', String(price))], { stdio: 'ignore' });
  return true;
}

const height = async (base) => Number((await (await fetch(`${base}/v4/height`)).json()).height);

/**
 * Whether `follower`, subscribed to the markets channel of `base`, is still being served: an
 * update reaches it after a tick, or, with no way to tick, it is answered and the height moves.
 */
async function served(follower, base) {
  const updates = () => follower.messages.filter((m) => m.type === 'channel_batch_data' && m.channel === 'v4_markets').length;
  const before = updates();
  if (tick()) return until(() => updates() > before, 20_000);
  const from = await height(base);
  const pongs = follower.messages.filter((m) => m.type === 'pong').length;
  follower.send({ type: 'ping' });
  return until(async () => follower.messages.filter((m) => m.type === 'pong').length > pongs && (await height(base)) > from + 2, 20_000);
}

const healthy = async (base) => {
  try {
    return (await fetch(`${base}/health`)).ok;
  } catch {
    return false;
  }
};

/** A WebSocket client that records what it is sent. */
function client(base) {
  const socket = new WebSocket(`${base.replace(/^http/, 'ws')}/v4/ws`);
  const state = { socket, messages: [], gaps: 0, last: -1, closed: null };
  socket.addEventListener('message', (event) => {
    const message = JSON.parse(event.data);
    if (message.message_id !== state.last + 1) state.gaps += 1;
    state.last = message.message_id;
    state.messages.push(message);
  });
  socket.addEventListener('close', (event) => {
    state.closed = { code: event.code, reason: event.reason };
  });
  state.opened = new Promise((resolve, reject) => {
    socket.addEventListener('open', resolve);
    socket.addEventListener('error', reject);
  });
  state.send = (message) => socket.send(JSON.stringify(message));
  state.has = (type, channel) => state.messages.some((m) => m.type === type && (!channel || m.channel === channel));
  return state;
}

// -------------------------------------------------------------------------------- the API

const log = openSync(args.log, 'a');
const api = spawn(
  args['api-binary'],
  [
    '--database-url', args['database-url'],
    '--deployment', args.deployment,
    '--listen-address', `127.0.0.1:${PORT}`,
    '--metrics-address', `127.0.0.1:${METRICS_PORT}`,
    '--max-round-checkpoints', String(MAX_ROUND_CHECKPOINTS),
    '--ws-send-timeout-secs', '2',
    '--ws-ping-interval-secs', String(PING_INTERVAL_S),
    '--ws-pong-timeout-secs', String(PONG_TIMEOUT_S),
    '--ws-message-rate', '50',
    '--ws-message-burst', '100',
  ],
  { stdio: ['ignore', log, log] }
);
process.on('exit', () => api.kill('SIGINT'));
check('the API under test comes up healthy', await until(() => healthy(OWN), 20_000));

// ---------------------------------------------------------------- a client that floods

{
  const flooder = client(OWN);
  await flooder.opened;
  for (let i = 0; i < 400; i += 1) flooder.send({ type: 'ping' });
  await until(() => flooder.closed, 5_000);
  check('a client sending too many messages is disconnected', flooder.closed?.code === 1008, JSON.stringify(flooder.closed));
  const pongs = flooder.messages.filter((m) => m.type === 'pong').length;
  check('it was answered up to its budget and no further', pongs >= 100 && pongs < 400, `${pongs} pongs`);
  const m = await metrics();
  check('the disconnect is counted', m['perp_api_ws_closed_total{reason="rate_limited"}'] === 1);
}

// ------------------------------------------------- a peer that has gone without closing

{
  // A peer whose network dropped looks idle, and writes to it keep succeeding. This one reads
  // what it is sent but never answers a ping, next to a client that does.
  const polite = client(OWN);
  await polite.opened;
  const silent = await rawClient();
  silent.on('data', () => {});
  let ended = null;
  const started = Date.now();
  silent.once('close', () => { ended = Date.now() - started; });
  check('the silent peer and the polite client are connected', (await metrics()).perp_api_ws_connections === 2);

  await until(() => ended !== null, (PING_INTERVAL_S + PONG_TIMEOUT_S) * 1000 + 5_000);
  const floor = (PING_INTERVAL_S + PONG_TIMEOUT_S) * 1000 - 500;
  check('a peer that answers no ping is given up on, one ping and one timeout later', ended !== null && ended >= floor, `closed after ${ended} ms`);
  console.log(`    (given up on after ${ended} ms)`);
  // Two more ping intervals, each of which the polite client answers.
  await sleep(2 * PING_INTERVAL_S * 1000);
  const m = await metrics();
  check('it is counted as unresponsive', m['perp_api_ws_closed_total{reason="unresponsive"}'] === 1, JSON.stringify(Object.entries(m).filter(([k]) => k.includes('ws_closed'))));
  check('a client that answers its pings stays connected', !polite.closed && m.perp_api_ws_connections === 1, JSON.stringify(polite.closed));
  polite.socket.close();
  await until(async () => (await metrics()).perp_api_ws_connections === 0, 5_000);
}

// ------------------------------------------------------- a client that stops reading

{
  // It asks for the tape again and again and never reads the answers: they pile up until the
  // server can write no more. It keeps sending pongs, so it is the writes that give it away
  // and not the heartbeat.
  const socket = await rawClient();
  socket.pause();

  const before = await metrics();
  check('the stalled client is connected', before.perp_api_ws_connections === 1, `${before.perp_api_ws_connections} connections`);
  const subscribe = frame(JSON.stringify({ type: 'subscribe', channel: 'v4_trades', id: TICKER }));
  const unsubscribe = frame(JSON.stringify({ type: 'unsubscribe', channel: 'v4_trades', id: TICKER }));
  const started = Date.now();
  let closedAfter = null;
  // Paced under the message budget: this client is slow, not abusive.
  while (Date.now() - started < 120_000) {
    if (socket.destroyed) break;
    socket.write(Buffer.concat([subscribe, unsubscribe, frame('', PONG)]));
    await sleep(50);
    const m = await metrics();
    if (m.perp_api_ws_connections === 0) {
      closedAfter = Date.now() - started;
      break;
    }
  }
  const after = await metrics();
  check('a client that stops reading is disconnected', closedAfter !== null, 'still connected after two minutes');
  check('the server gave up on a write rather than queueing', after['perp_api_ws_closed_total{reason="gone"}'] >= 1, JSON.stringify(Object.entries(after).filter(([k]) => k.includes('ws_closed'))));
  console.log(`    (disconnected after ${closedAfter} ms)`);
  socket.destroy();
}

// ------------------------------------------------------ the feed falls too far behind

{
  const follower = client(OWN);
  await follower.opened;
  follower.send({ type: 'subscribe', channel: 'v4_markets', batched: true });
  follower.send({ type: 'subscribe', channel: 'v4_orderbook', id: TICKER, batched: true });
  await until(() => follower.has('subscribed', 'v4_orderbook'), 5_000);
  check('a client is subscribed before the stall', follower.has('subscribed', 'v4_markets') && follower.has('subscribed', 'v4_orderbook'));

  // The process is frozen for longer than the checkpoints its feed bridges take to pass.
  const before = await metrics();
  api.kill('SIGSTOP');
  await sleep(FREEZE_MS);
  api.kill('SIGCONT');
  await until(() => follower.closed, 10_000);
  check('after a stall wider than the feed bridges, clients are told to start over', follower.closed?.code === 1012, JSON.stringify(follower.closed));
  const after = await metrics();
  check('the reset is counted', after.perp_api_feed_resets_total === before.perp_api_feed_resets_total + 1, `${before.perp_api_feed_resets_total} -> ${after.perp_api_feed_resets_total}`);
  check('the stream had no gap up to the reset', follower.gaps === 0);

  const again = client(OWN);
  await again.opened;
  again.send({ type: 'subscribe', channel: 'v4_orderbook', id: TICKER, batched: true });
  again.send({ type: 'subscribe', channel: 'v4_markets', batched: true });
  await until(() => again.has('subscribed', 'v4_markets'), 10_000);
  check('a client that starts over is subscribed again', again.has('subscribed', 'v4_orderbook') && again.has('subscribed', 'v4_markets'));
  check('and is served from then on', (await served(again, OWN)) && !again.closed && again.gaps === 0, JSON.stringify({ closed: again.closed, gaps: again.gaps }));
  again.socket.close();
}

api.kill('SIGINT');
check('the API under test shuts down on a signal', await until(() => api.exitCode !== null, 10_000), `exit code ${api.exitCode}`);
check('and exits cleanly', api.exitCode === 0, `exit code ${api.exitCode}`);

// ------------------------------------------------- the database drops its connections

{
  const base = args.api.replace(/\/$/, '');
  check('the long-running API is healthy', await healthy(base));
  const follower = client(base);
  await follower.opened;
  follower.send({ type: 'subscribe', channel: 'v4_markets', batched: true });
  await until(() => follower.has('subscribed', 'v4_markets'), 5_000);
  const before = await height(base);

  // Every connection of the API to its database. `run_all.sh` starts it with an application
  // name in its database URL, which tells its connections from the indexer's.
  const killed = execFileSync('psql', [
    args['database-url'],
    '-AtXc',
    "SELECT COUNT(pg_terminate_backend(pid)) FROM pg_stat_activity WHERE datname = current_database() AND application_name = 'perp-api'",
  ], { encoding: 'utf8' }).trim();
  check('the database dropped the API\'s connections', Number(killed) > 0, `${killed} connections`);

  const recovered = await until(async () => (await healthy(base)) && (await height(base)) > before + 4, 20_000, 250);
  check('the API recovers and keeps following the chain', recovered, `height ${await height(base)} from ${before}`);
  check('a client that stayed connected is still served, without a gap', (await served(follower, base)) && follower.gaps === 0 && !follower.closed, JSON.stringify({ gaps: follower.gaps, closed: follower.closed }));
  const rest = await fetch(`${base}/v4/trades/perpetualMarket/${TICKER}`);
  check('and REST answers from the database again', rest.ok);
  follower.socket.close();
}

const failed = results.filter(([, ok]) => !ok);
console.log(`\n${results.length - failed.length}/${results.length} checks passed`);
process.exit(failed.length === 0 ? 0 : 1);
