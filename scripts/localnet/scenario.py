#!/usr/bin/env python3
# Copyright (c) 2026 Geunhwa Jeong
# SPDX-License-Identifier: Apache-2.0

"""Drive a local network through the engine paths the indexer must handle.

    scripts/localnet/scenario.py publish --perp-dex <checkout>   # prints the --package list
    scripts/localnet/scenario.py run     --perp-dex <checkout>   # everything else

`publish` deploys the engine so the indexer can be started with the package addresses; `run`
then sets up a market and produces, while the indexer is following the chain: administrator
parameter changes, integrator fees, stop order and TWAP tickets through every step of their
life, a liquidation whose bad debt is socialized, and an auto-deleveraging.

Reuses the helpers of the engine's own suite (`<checkout>/e2e/localnet_e2e.py`), which must
point at the checkout the packages are published from. Needs a running localnet, the CLI on the
active `local` env, and grpcurl. Refuses to run anywhere but a local chain.
"""

import argparse
import hashlib
import json
import os
import sys
from pathlib import Path

STATE_FILE = Path(".localnet-scenario.json")


def load_lib(perp_dex):
    sys.path.insert(0, str(Path(perp_dex).expanduser() / "e2e"))
    import localnet_e2e as lib  # noqa: E402

    return lib


# --------------------------------------------------------------- BCS for ticket commitments


def bcs_u8(v):
    return v.to_bytes(1, "little")


def bcs_u64(v):
    return v.to_bytes(8, "little")


def bcs_u256(v):
    return v.to_bytes(32, "little")


def bcs_bool(v):
    return b"\x01" if v else b"\x00"


def bcs_option(v, enc):
    return b"\x00" if v is None else b"\x01" + enc(v)


def bcs_bytes(v):
    assert len(v) < 128
    return bytes([len(v)]) + v


def bcs_id(hex_id):
    return bytes.fromhex(hex_id[2:].rjust(64, "0"))


def blake2b(data):
    return hashlib.blake2b(data, digest_size=32).digest()


def stop_details(ch, expire, is_limit, trigger_type, stop_price, ge, side, size, price, order_type, reduce_only, salt):
    """`blake2b256(bcs(order details) || salt)` for a standalone stop order."""
    details = (
        bcs_id(ch)
        + bcs_option(expire, bcs_u64)
        + bcs_bool(is_limit)
        + bcs_u8(trigger_type)
        + bcs_u256(stop_price)
        + bcs_bool(ge)
        + bcs_bool(side)
        + bcs_u64(size)
        + bcs_u64(price)
        + bcs_u64(order_type)
        + bcs_bool(reduce_only)
        + b"\x00"  # Option<IntegratorInfo>::none
        + salt
    )
    return blake2b(details)


TWAP = dict(
    first_run_expire=None,
    expire=None,
    execution_gap_ms=0,
    execution_time_uncertainty_ms=60_000,
    chunks=2,
    small_tail_merge_threshold_bps=0,
    time_for_retry_ms=600_000,
    amount_uncertainty_bps=0,
    max_one_execution_amount_bps=10_000,
    max_slippage_bps=500,
)


def twap_details(side, size, reduce_only, salt):
    """`blake2b256(bcs(TWAPOrderDetails))`."""
    d = TWAP
    details = (
        bcs_option(d["first_run_expire"], bcs_u64)
        + bcs_option(d["expire"], bcs_u64)
        + bcs_u64(d["execution_gap_ms"])
        + bcs_u64(d["execution_time_uncertainty_ms"])
        + bcs_u64(d["chunks"])
        + bcs_u64(d["small_tail_merge_threshold_bps"])
        + bcs_u64(d["time_for_retry_ms"])
        + bcs_u64(d["amount_uncertainty_bps"])
        + bcs_u64(d["max_one_execution_amount_bps"])
        + bcs_bool(side)
        + bcs_u64(size)
        + bcs_u64(d["max_slippage_bps"])
        + bcs_bool(reduce_only)
        + b"\x00"  # Option<IntegratorInfo>::none
        + bcs_bytes(salt)
    )
    return blake2b(details)


# --------------------------------------------------------------- the scenario


def vec_u8(data):
    return ["--make-move-vec", "<u8>", "[" + ", ".join(f"{b}u8" for b in data) + "]"]


def publish(lib):
    lib.safety_check()
    ids = lib.publish_all()
    packages = {name: v["pkg"] for name, v in ids.items()}
    tx = {name: v["tx"] for name, v in ids.items()}
    STATE_FILE.write_text(json.dumps({"packages": packages, "publish_tx": tx}))
    print("\n--package " + " --package ".join(f"{n}={p}" for n, p in packages.items()))


def run(lib):
    lib.safety_check()
    state = json.loads(STATE_FILE.read_text())
    P, ids = state["packages"], state["publish_tx"]
    call, ptb, obj, u8, u16, u64, u128, u256, b, fx, px = (
        lib.call, lib.ptb, lib.obj, lib.u8, lib.u16, lib.u64, lib.u128, lib.u256, lib.b, lib.fx, lib.px,
    )
    events, section, check, ONE, B9, TUSD_UNIT, CLOCK = (
        lib.events, lib.section, lib.check, lib.ONE, lib.B9, lib.TUSD_UNIT, lib.CLOCK,
    )
    me = lib.cli("client", "active-address").stdout.strip()
    AUTH, VENDOR, ORACLE, PERP, ORDERS, E2E = (
        P["authority_cap"], P["vendor"], P["oracle_aggregator"], P["perpetuals"], P["perpetuals_orders"], P["perp_e2e"],
    )
    ADMIN = f"{AUTH}::authority::ADMIN"
    TUSD = f"{E2E}::tusd::TUSD"
    VK = f"{E2E}::vendor_key::E2E"
    ASK, BID, GTC, IOC = lib.ASK, lib.BID, lib.GTC, lib.IOC

    vendor_config = lib.shared_created(ids["vendor"], "::config::Config")
    vendor_pkg_admin = lib.owned_created(ids["vendor"], "::authority::AuthorityCap<")
    oracle_config = lib.shared_created(ids["oracle_aggregator"], "::config::Config")
    oracle_pkg_admin = lib.owned_created(ids["oracle_aggregator"], "::authority::AuthorityCap<")
    registry = lib.shared_created(ids["perpetuals"], "::registry::Registry")
    perp_pkg_admin = lib.owned_created(ids["perpetuals"], "::authority::AuthorityCap<")
    tusd_treasury = lib.owned_created(ids["perp_e2e"], "::coin::TreasuryCap<", "::tusd::TUSD>")
    tusd_metadata = next(
        c["objectId"] for c in ids["perp_e2e"]["objectChanges"]
        if c["type"] == "created" and c["objectType"].endswith("::tusd::TUSD>") and "::coin::CoinMetadata<" in c["objectType"]
    )

    # ------------------------------------------------------------ vendor, oracle, market
    section("Vendor, oracle and market")
    j = ptb("register vendor", call(f"{VENDOR}::config::register_vendor", [VK, ADMIN], obj(vendor_config), obj(vendor_pkg_admin), obj(me)))
    vendor_vk_cap = lib.owned_created(j, "::authority::AuthorityCap<")

    BTC0 = fx(100_000)
    cmds = call(f"{VENDOR}::metadata::new", [VK, ADMIN], obj(vendor_config), obj(vendor_vk_cap), "'Indexer scenario'", "'perp-indexer localnet scenario'", assign="meta")
    cmds += call(f"{VENDOR}::metadata::approve_domain_registration", [VK, f"{ORACLE}::authority::PACKAGE"], "meta", obj(vendor_config), obj(oracle_pkg_admin))
    cmds += call(f"{ORACLE}::config::register_vendor", [VK, ADMIN], obj(oracle_config), obj(vendor_vk_cap), obj(vendor_config), "meta", assign="oracle_vk")
    cmds += call(f"{PERP}::registry::set_vendor_registration", [], obj(registry), obj(perp_pkg_admin), "true")
    cmds += call(f"{PERP}::registry::authorize_extension", [f"{ORDERS}::extension::ORDERS"], obj(registry), obj(perp_pkg_admin))
    cmds += call(f"{PERP}::registry::register_vendor", [VK, ADMIN], obj(registry), obj(vendor_vk_cap), obj(vendor_config), "meta", assign="perp_vk")
    cmds += call(f"{PERP}::registry::create_vendor_treasury_cap", [VK], obj(registry), "perp_vk", assign="treasury")
    cmds += call(f"{PERP}::registry::create_package_adl_cap", [], obj(registry), obj(perp_pkg_admin), assign="adl")
    cmds += call(f"{E2E}::mock_source::create", [ADMIN], obj(oracle_config), obj(oracle_pkg_admin), assign="src")
    cmds += call(f"{E2E}::mock_source::authorize", [ADMIN], "src", obj(oracle_config), obj(oracle_pkg_admin))
    cmds += call(f"{ORACLE}::price_feed_storage::new", [VK, ADMIN], obj(oracle_config), "oracle_vk", "'BTC/USD'", assign="pfs_btc")
    cmds += call(f"{ORACLE}::price_feed_storage::new", [VK, ADMIN], obj(oracle_config), "oracle_vk", "'TUSD/USD'", assign="pfs_tusd")
    cmds += call(f"{E2E}::mock_source::new_price_feed", [VK, ADMIN], "src", "oracle_vk", obj(oracle_config), "pfs_btc", u128(BTC0), u64(1), CLOCK)
    cmds += call(f"{E2E}::mock_source::new_price_feed", [VK, ADMIN], "src", "oracle_vk", obj(oracle_config), "pfs_tusd", u128(ONE), u64(1), CLOCK)
    cmds += ["--make-move-vec", f"<{ORACLE}::price_feed_storage::PriceFeedStorage>", "[pfs_btc, pfs_tusd]", "--assign", "pfs_vec"]
    cmds += call(f"{ORACLE}::price_feed_storage::share_vec", [], "pfs_vec")
    cmds += ["--transfer-objects", "[meta, oracle_vk, perp_vk, treasury, adl, src]", obj(me)]
    j = ptb("vendor registration, oracle source, price feeds, ADL cap", cmds)
    source_id = int(events(j, "::events::CreatedSource")[0]["source_id"])
    storages = {e["symbol"]: e for e in events(j, "::events::CreatedPriceFeedStorage")}
    pfs_btc, pfs_tusd = storages["BTC/USD"]["price_feed_storage_obj_id"], storages["TUSD/USD"]["price_feed_storage_obj_id"]
    perp_vk = lib.owned_created(j, f"AuthorityCap<{PERP}::authority::VENDOR<{VK}>, {ADMIN}>")
    perp_treasury = lib.owned_created(j, f"{PERP}::authority::TREASURY>")
    adl_cap = lib.owned_created(j, f"{PERP}::authority::ADL>")
    source = lib.owned_created(j, "::source::Source<")

    IMR, MMR, LOT, TICK = lib.IMR, lib.MMR, lib.LOT, lib.TICK
    MAKER_FEE, TAKER_FEE, LIQ_FEE, IF_FEE = lib.MAKER_FEE, lib.TAKER_FEE, lib.LIQ_FEE, lib.IF_FEE
    cmds = call(f"{PERP}::clearing_house::create_orderbook", [VK, ADMIN], obj(perp_vk), obj(registry), u64(2), u64(4), u64(4), u64(2), u64(3), u64(4), assign="ob")
    cmds += call(f"{PERP}::market::new_creation_params", [], u256(IMR), u256(MMR), u64(LOT), u64(TICK), u256(0), u256(0), assign="params")
    cmds += call(f"{PERP}::market::set_fees", [], "params", u256(MAKER_FEE), u256(TAKER_FEE), u256(LIQ_FEE), u256(IF_FEE))
    cmds += call(f"{PERP}::market::set_funding", [], "params", u64(60_000), u64(21_600_000))
    cmds += call(f"{PERP}::market::set_premium_twap", [], "params", u64(1_000), u64(60_000))
    cmds += call(f"{PERP}::market::set_spread_twap", [], "params", u64(1_000), u64(60_000))
    cmds += call(f"{PERP}::clearing_house::create_clearing_house", [TUSD, VK, ADMIN], "ob", obj(perp_vk), obj(registry), obj(tusd_metadata), CLOCK, obj(pfs_btc), obj(pfs_tusd), u16(source_id), u16(source_id), "params", assign="ch")
    cmds += call(f"{PERP}::clearing_house::register_market", [VK, ADMIN, TUSD], obj(registry), obj(perp_vk), "ch")
    cmds += call(f"{PERP}::clearing_house::share", [TUSD], "ch")
    j = ptb("create BTC/USD clearing house", cmds)
    ch = lib.shared_created(j, f"::clearing_house::ClearingHouse<{TUSD}>")
    check("clearing house created", len(events(j, "::events::CreatedClearingHouse")) == 1)

    # ------------------------------------------------------------ administrator parameters
    section("Administrator parameter changes")
    cmds = call(f"{PERP}::clearing_house::set_fee_params", [VK, ADMIN, TUSD], obj(ch), obj(perp_vk), obj(registry), f"some({u256(MAKER_FEE)})", "none", "none", "none", "none")
    cmds += call(f"{PERP}::clearing_house::set_twap_params", [VK, ADMIN, TUSD], obj(ch), obj(perp_vk), obj(registry), "none", "none", f"some({u64(1_000)})", "none", "none", "none", CLOCK)
    cmds += call(f"{PERP}::clearing_house::set_core_params", [VK, ADMIN, TUSD], obj(ch), obj(perp_vk), obj(registry), f"some({u64(LOT)})", "none", "none")
    # Allow bad debt to be socialized: up to 100,000 USD and a 5% margin ratio drop.
    cmds += call(f"{PERP}::clearing_house::set_risk_limit_params", [VK, ADMIN, TUSD], obj(ch), obj(perp_vk), obj(registry),
                 "none", f"some({u64(100)})", "none", "none", "none", "none", "none", f"some({u256(fx(100_000))})", f"some({u256(ONE // 20)})", "none")
    cmds += call(f"{PERP}::clearing_house::set_base_oracle_params", [VK, ADMIN, TUSD], obj(ch), obj(perp_vk), obj(registry), obj(pfs_btc), "none", f"some({u64(600_000)})")
    cmds += call(f"{PERP}::clearing_house::set_collateral_oracle_params", [TUSD, ADMIN], obj(ch), obj(perp_pkg_admin), obj(registry), obj(pfs_tusd), "none", f"some({u64(600_000)})")
    # Proposals normally mature after a day at least; allow one that matures in a millisecond.
    cmds += call(f"{PERP}::registry::new_config_update", [], assign="upd")
    cmds += call(f"{PERP}::registry::set_proposal_and_order_value_bounds", [], "upd", u64(1), u64(259_200_000), u256(ONE // 2), u256(fx(1_000)))
    cmds += call(f"{PERP}::registry::apply_config_update", [ADMIN], obj(registry), obj(perp_pkg_admin), "upd")
    cmds += call(f"{PERP}::clearing_house::create_margin_ratios_proposal", [VK, ADMIN, TUSD], obj(ch), obj(perp_vk), obj(registry), u64(1), u256(IMR), u256(MMR), CLOCK)
    cmds += call(f"{PERP}::registry::register_integrator", [], obj(registry))
    cmds += call(f"{PERP}::registry::set_integrator_address", [], obj(registry), u32_(0), obj(me))
    cmds += call("0x2::coin::mint", [TUSD], obj(tusd_treasury), u64(1_000 * TUSD_UNIT), assign="donation")
    cmds += call(f"{PERP}::clearing_house::donate_to_insurance_fund", [TUSD], obj(ch), "donation")
    cmds += call(f"{PERP}::clearing_house::withdraw_insurance_fund", [TUSD, VK], obj(ch), obj(perp_treasury), obj(registry), obj(pfs_btc), obj(pfs_tusd), CLOCK, u64(999 * TUSD_UNIT), assign="back")
    cmds += ["--transfer-objects", "[back]", obj(me)]
    j = ptb("parameter setters, margin ratio proposal, integrator, insurance fund", cmds)
    for name in ["SetFeeParams", "SetTwapParams", "SetCoreParams", "SetRiskLimitParams", "SetBaseOracleParams",
                 "SetCollateralOracleParams", "UpdatedIntegratorAddress", "DonatedToInsuranceFund", "WithdrewInsuranceFund"]:
        check(f"{name} emitted", len(events(j, f"::events::{name}")) == 1)

    # ------------------------------------------------------------ accounts
    section("Accounts")
    deposits = {"M": 1_000_000, "T": 100_000, "V": 10_000, "L": 200_000}
    allocs = {"M": 500_000, "T": 20_000, "V": 3_000, "L": 100_000}
    cmds = []
    for name, amount in deposits.items():
        cmds += call("0x2::coin::mint", [TUSD], obj(tusd_treasury), u64(amount * TUSD_UNIT), assign=f"coin{name}")
        cmds += call(f"{PERP}::account::create_account", [TUSD], obj(registry), assign=f"acc{name}")
        cmds += call(f"{PERP}::account::deposit_collateral", [TUSD, ADMIN], f"acc{name}.0", f"acc{name}.2", obj(registry), f"coin{name}")
        cmds += call(f"{PERP}::account::consume_policy_and_share_account", [TUSD], f"acc{name}.0", f"acc{name}.1")
    cmds += ["--transfer-objects", "[" + ", ".join(f"acc{n}.2" for n in deposits) + "]", obj(me)]
    cmds += call(f"{PERP}::clearing_house::commit_margin_ratios_proposal", [TUSD], obj(ch), CLOCK)
    j = ptb("create and fund four accounts, commit the margin ratio proposal", cmds)
    check("UpdatedMarginRatios emitted", len(events(j, "::events::UpdatedMarginRatios")) == 1)
    created = events(j, "::events::CreatedAccount")
    caps = [c["objectId"] for c in j["objectChanges"] if c["type"] == "created" and f"AuthorityCap<{PERP}::authority::ACCOUNT, {ADMIN}>" in c["objectType"]]
    cap_for = {}
    for cid in caps:
        o = json.loads(lib.cli("client", "object", cid, "--json").stdout)
        cap_for[o["content"]["for"]] = cid
    acct = {}
    for name, ev in zip(deposits, created):
        acct[name] = dict(id=int(ev["account_id"]), obj=ev["account_obj_id"], cap=cap_for[ev["account_obj_id"]])
    cmds = []
    for name, a in acct.items():
        cmds += call(f"{PERP}::clearing_house::create_market_position", [TUSD, ADMIN], obj(ch), obj(a["cap"]), obj(a["obj"]))
        cmds += call(f"{PERP}::clearing_house::allocate_collateral", [TUSD, ADMIN], obj(ch), obj(a["cap"]), obj(a["obj"]), u64(allocs[name] * TUSD_UNIT))
    for name in ("T", "V"):
        cmds += call(f"{PERP}::clearing_house::set_position_initial_margin_ratio", [TUSD, ADMIN], obj(ch), obj(acct[name]["cap"]), obj(acct[name]["obj"]), u256(IMR))
    # T lets integrator 0 charge it up to 0.1%.
    cmds += call(f"{PERP}::account::add_integrator_config", [TUSD, ADMIN], obj(acct["T"]["obj"]), obj(acct["T"]["cap"]), obj(registry), u32_(0), u32_(1_000_000))
    ptb("positions, allocations, 10x leverage for T and V, T's integrator", cmds)

    btc_price = [BTC0]

    def refresh_prices():
        return (call(f"{E2E}::mock_source::set_price", [], obj(source), obj(oracle_config), obj(pfs_btc), u128(btc_price[0]), CLOCK)
                + call(f"{E2E}::mock_source::set_price", [], obj(source), obj(oracle_config), obj(pfs_tusd), u128(ONE), CLOCK))

    def session(label, who, actions, integrator=None, pre=None, expect_abort=None):
        a = acct[who]
        cmds = list(pre or []) + refresh_prices()
        if integrator is None:
            cmds += call("0x1::option::none", [f"{PERP}::account::IntegratorInfo"], assign="integrator")
        else:
            cmds += call(f"{PERP}::account::create_integrator_info", [], u32_(integrator[0]), u32_(integrator[1]), assign="integrator")
        cmds += call(f"{PERP}::clearing_house::start_session", [TUSD, ADMIN], obj(ch), obj(a["cap"]), obj(a["obj"]), obj(pfs_btc), obj(pfs_tusd), "integrator", CLOCK, assign="hp")
        for act in actions:
            cmds += act
        cmds += call(f"{PERP}::clearing_house::end_session", [TUSD, ADMIN], "hp", obj(a["cap"]), obj(a["obj"]), "false", "false", assign="res")
        cmds += call(f"{PERP}::clearing_house::share", [TUSD], "res.0")
        return ptb(label, cmds, expect_abort=expect_abort)

    def limit(side, size, price, otype=GTC, reduce_only=False):
        return call(f"{PERP}::clearing_house::place_limit_order", [TUSD], "hp", b(side), u64(size), u64(price), u64(otype), "none", b(reduce_only), "none")

    def market_order(side, size, reduce_only=False):
        return call(f"{PERP}::clearing_house::place_market_order", [TUSD], "hp", b(side), u64(size), b(reduce_only))

    m_orders = []

    def ladder(center, size=200_000_000):
        asks = [center + 10 * i for i in range(1, 7)]
        bids = [center - 10 * i for i in range(1, 7)]
        j = session(f"M posts a ladder around {center:,}", "M", [limit(ASK, size, px(p)) for p in asks] + [limit(BID, size, px(p)) for p in bids])
        m_orders.extend(e["order_id"] for e in events(j, "::events::PostedOrder"))
        return j

    def clear_book():
        """Cancels whatever is left of M's ladders, so the mark price follows the index."""
        cmds = ["--make-move-vec", "<u128>", "[" + ", ".join(f"{i}u128" for i in m_orders) + "]", "--assign", "oids"]
        cmds += call(f"{PERP}::clearing_house::try_cancel_orders", [TUSD, ADMIN], obj(ch), obj(acct["M"]["cap"]), obj(acct["M"]["obj"]), "oids")
        ptb("M clears the book", cmds)
        m_orders.clear()

    def gas_coin(amount, name):
        return ["--split-coins", "gas", f"[{amount}]", "--assign", name]

    def executor(name="executor"):
        return call(f"{PERP}::clearing_house::no_domain_executor", [], assign=name)

    def no_integrator(name):
        return call("0x1::option::none", [f"{PERP}::account::IntegratorInfo"], assign=name)

    # ------------------------------------------------------------ trading with an integrator
    section("Ladder, taker with an integrator, resting order and cancel")
    ladder(100_000)
    j = session("T buys 0.25 through integrator 0 at 0.1%", "T", [market_order(BID, 250_000_000)], integrator=(0, 1_000_000))
    taker = events(j, "::events::FilledTakerOrder")[0]
    check("taker fill carries the integrator", taker["integrator_id"] == 0 and int(taker["integrator_fee_paid_usd"]) > 0)
    j = session("T rests a bid with the integrator", "T", [limit(BID, 50_000_000, px(99_900))], integrator=(0, 1_000_000))
    posted = events(j, "::events::PostedOrder")[0]
    check("posted order carries the integrator", posted["integrator_id"] == 0)
    cmds = ["--make-move-vec", "<u128>", f"[{posted['order_id']}u128]", "--assign", "oids"]
    cmds += call(f"{PERP}::clearing_house::cancel_orders", [TUSD, ADMIN], obj(ch), obj(acct["T"]["cap"]), obj(acct["T"]["obj"]), "oids")
    j = ptb("T cancels its bid", cmds)
    check("CanceledOrder by the user", events(j, "::events::CanceledOrder")[0]["cancelation_reason"] == 0)

    # ------------------------------------------------------------ stop orders
    section("Stop order tickets")
    stop = dict(expire=None, is_limit=False, trigger_type=0, stop_price=fx(99_000), ge=False, side=ASK, size=50_000_000, price=px(1), order_type=IOC, reduce_only=True)
    salt1, salt2 = bytes(range(1, 9)), bytes(range(11, 19))
    commit1 = stop_details(ch, **stop, salt=salt1)
    cmds = gas_coin(100_000_000, "g")
    cmds += ["--make-move-vec", "<address>", f"[{obj(me)}]", "--assign", "execs"]
    cmds += vec_u8(commit1) + ["--assign", "commit"]
    cmds += call(f"{ORDERS}::stop_orders::create_stop_order_ticket", [TUSD, ADMIN], obj(acct["T"]["obj"]), obj(acct["T"]["cap"]), obj(registry), "execs", "none", "g.0", u64(1), "commit")
    j = ptb("T creates a stop ticket: sell 0.05 if the index drops to 99,000", cmds)
    ticket1 = events(j, "::events::CreatedStopOrderTicket")[0]["ticket_id"]

    commit2 = stop_details(ch, **stop, salt=salt2)
    cmds = vec_u8(commit2) + ["--assign", "commit"]
    cmds += call(f"{ORDERS}::stop_orders::edit_stop_order_ticket_details", [TUSD, ADMIN], obj(acct["T"]["obj"]), obj(acct["T"]["cap"]), obj(registry), obj(ticket1), "commit")
    cmds += ["--make-move-vec", "<address>", f"[{obj(me)}, @0x1]", "--assign", "execs"]
    cmds += call(f"{ORDERS}::stop_orders::edit_stop_order_ticket_executors", [TUSD, ADMIN], obj(acct["T"]["obj"]), obj(acct["T"]["cap"]), obj(registry), obj(ticket1), "execs")
    j = ptb("T edits the ticket's details and executors", cmds)
    check("ticket edits emitted", len(events(j, "::events::EditedStopOrderTicketDetails")) == 1 and len(events(j, "::events::EditedStopOrderTicketExecutors")) == 1)

    btc_price[0] = fx(98_500)
    cmds = refresh_prices() + executor() + no_integrator("integrator") + vec_u8(salt2) + ["--assign", "salt"]
    cmds += call(f"{ORDERS}::stop_orders::place_stop_order_standalone", [TUSD], obj(ch), obj(pfs_btc), obj(pfs_tusd), CLOCK, obj(registry), obj(ticket1), obj(acct["T"]["obj"]),
                 "none", b(stop["is_limit"]), u8(stop["trigger_type"]), u256(stop["stop_price"]), b(stop["ge"]), b(stop["side"]), u64(stop["size"]), u64(stop["price"]),
                 u64(stop["order_type"]), b(stop["reduce_only"]), "salt", "integrator", "executor", assign="r")
    cmds += call(f"{PERP}::clearing_house::share", [TUSD], "r.2")
    cmds += ["--transfer-objects", "[r.1]", obj(me)]
    j = ptb("executor triggers the stop at 98,500", cmds)
    check("ExecutedStopOrderTicket and a taker fill", len(events(j, "::events::ExecutedStopOrderTicket")) == 1 and len(events(j, "::events::FilledTakerOrder")) == 1)

    cmds = gas_coin(100_000_000, "g") + ["--make-move-vec", "<address>", f"[{obj(me)}]", "--assign", "execs"] + vec_u8(commit1) + ["--assign", "commit"]
    cmds += call(f"{ORDERS}::stop_orders::create_stop_order_ticket", [TUSD, ADMIN], obj(acct["T"]["obj"]), obj(acct["T"]["cap"]), obj(registry), "execs", "none", "g.0", u64(1), "commit")
    j = ptb("T creates a second stop ticket", cmds)
    ticket2 = events(j, "::events::CreatedStopOrderTicket")[0]["ticket_id"]
    cmds = call(f"{ORDERS}::stop_orders::cancel", [TUSD, ADMIN], obj(acct["T"]["obj"]), obj(acct["T"]["cap"]), obj(registry), obj(ticket2), assign="g")
    cmds += ["--transfer-objects", "[g]", obj(me)]
    j = ptb("T cancels the second ticket", cmds)
    check("DeletedStopOrderTicket", len(events(j, "::events::DeletedStopOrderTicket")) == 1)

    # ------------------------------------------------------------ TWAP orders
    section("TWAP order tickets")
    btc_price[0] = BTC0
    ladder(100_000)
    twap_size, twap_salt, twap_salt2 = 100_000_000, bytes(range(21, 29)), bytes(range(31, 39))

    def new_details(salt, name="details"):
        d = TWAP
        return call(f"{ORDERS}::twap_orders::new_details", [], "none", "none", u64(d["execution_gap_ms"]), u64(d["execution_time_uncertainty_ms"]), u64(d["chunks"]),
                    u64(d["small_tail_merge_threshold_bps"]), u64(d["time_for_retry_ms"]), u64(d["amount_uncertainty_bps"]), u64(d["max_one_execution_amount_bps"]),
                    b(BID), u64(twap_size), u64(d["max_slippage_bps"]), "false", "no_integrator", "salt", assign=name)

    def create_twap(salt, label):
        cmds = gas_coin(100_000_000, "g") + ["--make-move-vec", "<address>", f"[{obj(me)}]", "--assign", "execs"]
        cmds += vec_u8(twap_details(BID, twap_size, False, salt)) + ["--assign", "commit"]
        cmds += call(f"{ORDERS}::twap_orders::create_twap_order_ticket", [TUSD, ADMIN], obj(acct["T"]["obj"]), obj(acct["T"]["cap"]), obj(ch), obj(registry), "execs", "none", "g.0", "commit")
        j = ptb(label, cmds)
        return events(j, "::events::CreatedTWAPOrderTicket")[0]["ticket_id"]

    def execute_twap(ticket, salt, amount, label):
        cmds = refresh_prices() + executor() + no_integrator("no_integrator") + vec_u8(salt) + ["--assign", "salt"] + new_details(salt)
        cmds += call(f"{ORDERS}::twap_orders::execute", [TUSD], obj(acct["T"]["obj"]), obj(ch), obj(pfs_btc), obj(pfs_tusd), obj(ticket), "details", u64(amount), CLOCK, obj(registry), "executor", assign="r")
        cmds += call(f"{PERP}::clearing_house::share", [TUSD], "r.2") + ["--transfer-objects", "[r.1]", obj(me)]
        return ptb(label, cmds)

    twap1 = create_twap(twap_salt, "T creates a TWAP ticket: buy 0.1 in two chunks")
    j = execute_twap(twap1, twap_salt, twap_size // 2, "executor runs the first chunk")
    check("ProcessedTWAPOrderTicket after the first chunk", len(events(j, "::events::ProcessedTWAPOrderTicket")) == 1)
    j = execute_twap(twap1, twap_salt, twap_size // 2, "executor runs the second chunk")
    cmds = refresh_prices() + executor() + no_integrator("no_integrator") + vec_u8(twap_salt) + ["--assign", "salt"] + new_details(twap_salt)
    cmds += call(f"{ORDERS}::twap_orders::finalize", [TUSD], obj(acct["T"]["obj"]), obj(ch), obj(pfs_btc), obj(pfs_tusd), CLOCK, obj(registry), obj(twap1), "details", "executor", assign="g")
    cmds += ["--transfer-objects", "[g]", obj(me)]
    j = ptb("executor finalizes the TWAP", cmds)
    check("FinalizedTWAPOrderTicket then DeletedTWAPOrderTicket", len(events(j, "::events::FinalizedTWAPOrderTicket")) == 1 and len(events(j, "::events::DeletedTWAPOrderTicket")) == 1)

    twap2 = create_twap(twap_salt, "T creates a second TWAP ticket")
    cmds = vec_u8(twap_details(BID, twap_size, False, twap_salt2)) + ["--assign", "commit"]
    cmds += call(f"{ORDERS}::twap_orders::set_details", [TUSD, ADMIN], obj(acct["T"]["obj"]), obj(acct["T"]["cap"]), obj(registry), obj(twap2), "commit")
    cmds += ["--make-move-vec", "<address>", f"[{obj(me)}, @0x1]", "--assign", "execs"]
    cmds += call(f"{ORDERS}::twap_orders::set_executors", [TUSD, ADMIN], obj(acct["T"]["obj"]), obj(acct["T"]["cap"]), obj(registry), obj(twap2), "execs")
    j = ptb("T edits the second TWAP's details and executors", cmds)
    check("TWAP edits emitted", len(events(j, "::events::EditedTWAPOrderTicketDetails")) == 1 and len(events(j, "::events::EditedTWAPOrderTicketExecutors")) == 1)
    cmds = refresh_prices()
    cmds += call(f"{ORDERS}::twap_orders::user_cancel_twap_order", [TUSD, ADMIN], obj(acct["T"]["obj"]), obj(acct["T"]["cap"]), obj(ch), obj(pfs_btc), obj(pfs_tusd), obj(twap2), CLOCK, obj(registry), assign="g")
    cmds += ["--transfer-objects", "[g]", obj(me)]
    j = ptb("T cancels the second TWAP", cmds)
    check("CanceledTWAPOrderTicket then DeletedTWAPOrderTicket", len(events(j, "::events::CanceledTWAPOrderTicket")) == 1 and len(events(j, "::events::DeletedTWAPOrderTicket")) == 1)

    # ------------------------------------------------------------ liquidation with socialized bad debt
    section("Liquidation with bad debt beyond the insurance fund")
    ladder(100_000)
    session("V buys 0.25 at 10x", "V", [market_order(BID, 250_000_000)])
    clear_book()
    cmds = ["--make-move-vec", "<u128>", "[]", "--assign", "ids"]
    btc_price[0] = fx(85_000)
    j = session("L liquidates V at 85,000", "L", [call(f"{PERP}::clearing_house::liquidate", [TUSD], "hp", u64(acct["V"]["id"]), "ids")], pre=cmds)
    liquidated = events(j, "::events::LiquidatedPosition")[0]
    check("liquidation left bad debt", int(liquidated["bad_debt"]) > 0, liquidated["bad_debt"])
    check("SocializedBadDebt emitted", len(events(j, "::events::SocializedBadDebt")) == 1)

    # ------------------------------------------------------------ auto-deleveraging
    section("Auto-deleveraging")
    # T is long about 0.3 BTC on 20,000 of collateral: at 30,000 its equity is negative.
    clear_book()
    btc_price[0] = fx(30_000)
    cmds = refresh_prices()
    cmds += ["--make-move-vec", "<u128>", "[]", "--assign", "open_orders"]
    cmds += ["--make-move-vec", "<u64>", f"[{u64(acct['M']['id'])}]", "--assign", "counterparties"]
    cmds += ["--make-move-vec", "<u64>", f"[{u64(300_000_000)}]", "--assign", "sizes"]
    cmds += ["--make-move-vec", "<u64>", f"[{u64(ONE)}]", "--assign", "weights"]
    cmds += call(f"{PERP}::adl::execute_adl", [TUSD], obj(ch), obj(adl_cap), obj(registry), u64(acct["T"]["id"]), "open_orders", "counterparties", "sizes", "weights", obj(pfs_btc), obj(pfs_tusd), CLOCK)
    j = ptb("ADL closes T against M at 30,000", cmds)
    check("PerformedADL emitted", len(events(j, "::events::PerformedADL")) == 1)

    failed = [n for n, ok in lib.RESULTS if not ok]
    print(f"\n{len(lib.RESULTS) - len(failed)}/{len(lib.RESULTS)} checks passed")
    if failed:
        sys.exit(1)


def u32_(n):
    return f"{n}u32"


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("command", choices=["publish", "run"])
    parser.add_argument("--perp-dex", required=True, help="engine checkout the packages are published from")
    args = parser.parse_args()
    lib = load_lib(args.perp_dex)
    os.chdir(Path(args.perp_dex).expanduser())
    (publish if args.command == "publish" else run)(lib)


if __name__ == "__main__":
    main()
