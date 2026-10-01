// Copyright (c) 2026 Geunhwa Jeong
// SPDX-License-Identifier: Apache-2.0

//! Turns a checkpoint into the changes it makes to the derived state.

use std::collections::HashSet;

use anyhow::Context;
use bigdecimal::{BigDecimal, Zero};
use haneul_indexer_alt_framework::types::effects::TransactionEffectsAPI;
use haneul_indexer_alt_framework::types::full_checkpoint_content::Checkpoint;
use haneul_indexer_alt_framework::types::object::{Object, Owner};
use move_core_types::account_address::AccountAddress;
use move_core_types::language_storage::{StructTag, TypeTag};
use perp_schema::models::{
    AccountCap, AccountSnapshot, CollateralTransfer, Fill, FundingPayment, FundingUpdate,
    MarketSnapshot, OraclePrice, Order, OrderTicket, PositionSnapshot,
};
use perp_types::objects::{Account, AuthorityCap, ClearingHouse, PositionField};
use perp_types::types::{Address, U256};
use perp_types::{
    DecodeError, EVENTS_MODULE, Package, PerpEvent, oracle_aggregator, perpetuals,
    perpetuals_orders,
};
use serde_json::json;
use tracing::warn;

use super::change::{Change, MarketPrices, OrderUpdate, TicketChange};
use crate::convert::{b9, ifixed, int8, integer, oracle_price, order_side_and_price};
use crate::packages::Packages;

/// Where an event sits on chain.
struct EventContext<'a> {
    checkpoint: i64,
    timestamp_ms: i64,
    tx_index: i64,
    event_index: i64,
    tx_digest: &'a str,
    /// The event type's first type parameter: the collateral coin, for events that have one.
    collateral_type: Option<String>,
}

/// The changes `checkpoint` makes, in chain order.
///
/// Within a transaction, the changes from its events come before the snapshots of the objects it
/// wrote, so that a row created by an event is completed by the state the transaction left.
pub fn changes(checkpoint: &Checkpoint, packages: &Packages) -> anyhow::Result<Vec<Change>> {
    let sequence_number = int8(checkpoint.summary.sequence_number)?;
    let timestamp_ms = int8(checkpoint.summary.timestamp_ms)?;

    let mut out = vec![];
    for (tx_index, tx) in checkpoint.transactions.iter().enumerate() {
        let tx_digest = tx.effects.transaction_digest().to_string();

        for (event_index, event) in tx.events.iter().flat_map(|e| &e.data).enumerate() {
            let Some(decoder) = packages.get(&event.type_.address).and_then(|p| p.decoder) else {
                continue;
            };
            if event.type_.module.as_str() != EVENTS_MODULE {
                continue;
            }
            let name = event.type_.name.as_str();
            let decoded = match PerpEvent::decode(decoder, name, &event.contents) {
                Ok(decoded) => decoded,
                // An event this version does not know cannot belong to any table it maintains.
                Err(DecodeError::UnknownEvent(_)) => {
                    warn!(package = %decoder, name, "Skipping unknown event");
                    continue;
                }
                // Applying later events on top of a skipped one would corrupt the state, so this
                // keeps failing (and holding the pipeline at this checkpoint) until the decoders
                // are fixed. The ledger pipeline is unaffected and keeps the raw bytes.
                Err(e) => {
                    return Err(e).with_context(|| {
                        format!("{decoder}::{name} in {tx_digest} (checkpoint {sequence_number})")
                    });
                }
            };
            let ctx = EventContext {
                checkpoint: sequence_number,
                timestamp_ms,
                tx_index: tx_index as i64,
                event_index: event_index as i64,
                tx_digest: &tx_digest,
                collateral_type: event.type_.type_params.first().map(type_name),
            };
            match decoded {
                PerpEvent::Perpetuals(event) => perpetuals_event(&ctx, event, &mut out)?,
                PerpEvent::PerpetualsOrders(event) => orders_event(&ctx, event, &mut out)?,
                PerpEvent::OracleAggregator(event) => oracle_event(&ctx, event, &mut out)?,
            }
        }

        let mut written = HashSet::new();
        for object in tx.output_objects(&checkpoint.object_set) {
            written.insert(object.id());
            object_snapshot(sequence_number, timestamp_ms, object, packages, &mut out)
                .with_context(|| format!("object {} written by {tx_digest}", object.id()))?;
        }
        // A capability the transaction read but did not write back was deleted or wrapped.
        for object in tx.input_objects(&checkpoint.object_set) {
            let is_cap = object
                .struct_tag()
                .is_some_and(|tag| account_cap_role(&tag, packages).is_some());
            if is_cap && !written.contains(&object.id()) {
                out.push(Change::AccountCapRemoved {
                    cap_id: object.id().to_canonical_string(/* with_prefix */ true),
                    checkpoint: sequence_number,
                });
            }
        }
    }
    Ok(out)
}

fn type_name(tag: &TypeTag) -> String {
    tag.to_canonical_string(/* with_prefix */ true)
}

fn id(address: &Address) -> String {
    address.to_string()
}

/// `quote / size`, the price a fill executed at. `None` for an empty fill.
fn price_of(quote: &BigDecimal, size: &BigDecimal) -> Option<BigDecimal> {
    (!size.is_zero()).then(|| (quote / size).round(18))
}

/// The index price the checkpoint's events have reported for `market` so far, if any.
fn last_index_price(changes: &[Change], market: &str) -> Option<BigDecimal> {
    changes.iter().rev().find_map(|change| match change {
        Change::MarketPrices(prices) if prices.market == market => prices.index_price.clone(),
        _ => None,
    })
}

fn perpetuals_event(
    ctx: &EventContext<'_>,
    event: perpetuals::Event,
    out: &mut Vec<Change>,
) -> anyhow::Result<()> {
    use perpetuals::Event as E;

    let prices =
        |market: &Address, mark: Option<&U256>, index: Option<&U256>, book: Option<BigDecimal>| {
            Change::MarketPrices(MarketPrices {
                market: id(market),
                mark_price: mark.map(ifixed),
                index_price: index.map(ifixed),
                book_price: book,
                timestamp_ms: ctx.timestamp_ms,
            })
        };
    let transfer = |account_id: u64, kind: &str, market: Option<&Address>, amount: u64| {
        anyhow::Ok(Change::CollateralTransfer(CollateralTransfer {
            checkpoint: ctx.checkpoint,
            tx_index: ctx.tx_index,
            event_index: ctx.event_index,
            tx_digest: ctx.tx_digest.to_owned(),
            timestamp_ms: ctx.timestamp_ms,
            account_id: int8(account_id)?,
            kind: kind.to_owned(),
            market: market.map(id),
            amount: integer(amount),
        }))
    };
    // A fill row with the fields every kind of fill shares.
    let fill = |fill_index: i64, market: &Address, account_id: u64, is_ask: bool| {
        anyhow::Ok(Fill {
            checkpoint: ctx.checkpoint,
            tx_index: ctx.tx_index,
            event_index: ctx.event_index,
            fill_index,
            tx_digest: ctx.tx_digest.to_owned(),
            timestamp_ms: ctx.timestamp_ms,
            market: id(market),
            account_id: int8(account_id)?,
            counterparty_account_id: None,
            is_ask,
            liquidity: "taker".to_owned(),
            kind: "trade".to_owned(),
            price: None,
            size: BigDecimal::zero(),
            quote: None,
            fee: BigDecimal::zero(),
            integrator_fee: BigDecimal::zero(),
            pnl: BigDecimal::zero(),
            order_id: None,
            client_order_id: None,
            mark_price: None,
            position_base_before: None,
            entry_price_before: None,
        })
    };

    match event {
        E::CreatedClearingHouse(e) => out.push(Change::MarketCreated {
            market: id(&e.ch_id),
            collateral_decimals: int8(e.coin_decimals)?,
            checkpoint: ctx.checkpoint,
            timestamp_ms: ctx.timestamp_ms,
        }),
        E::ClosedMarket(e) => out.push(Change::MarketClosed {
            market: id(&e.ch_id),
        }),
        E::UpdatedSettlementPrices(e) => out.push(Change::MarketSettlement {
            market: id(&e.ch_id),
            enabled: e.settlement_enabled,
            base_price: ifixed(&e.base_settlement_price),
            collateral_price: ifixed(&e.collateral_settlement_price),
        }),
        E::UpdatedPremiumTwap(e) => out.push(prices(
            &e.ch_id,
            None,
            Some(&e.index_price),
            Some(ifixed(&e.actual_book_price)),
        )),
        E::UpdatedSpreadTwap(e) => out.push(prices(
            &e.ch_id,
            None,
            Some(&e.index_price),
            Some(ifixed(&e.actual_book_price)),
        )),

        E::CreatedAccount(e) => out.push(Change::AccountCreated {
            account_id: int8(e.account_id)?,
            creator: id(&e.user),
            checkpoint: ctx.checkpoint,
            timestamp_ms: ctx.timestamp_ms,
        }),
        E::DepositedCollateral(e) => {
            out.push(transfer(e.account_id, "deposit", None, e.collateral)?)
        }
        E::WithdrewCollateral(e) => {
            out.push(transfer(e.account_id, "withdraw", None, e.collateral)?)
        }
        E::AllocatedCollateral(e) => out.push(transfer(
            e.account_id,
            "allocate",
            Some(&e.ch_id),
            e.collateral,
        )?),
        E::DeallocatedCollateral(e) => out.push(transfer(
            e.account_id,
            "deallocate",
            Some(&e.ch_id),
            e.collateral,
        )?),

        E::PostedOrder(e) => {
            let (is_ask, price) = order_side_and_price(e.order_id.0);
            let size = b9(e.order_size);
            out.push(Change::OrderPosted(Order {
                market: id(&e.ch_id),
                order_id: integer(e.order_id.0),
                account_id: int8(e.account_id)?,
                is_ask,
                price: b9(price),
                size: size.clone(),
                remaining: size,
                filled: BigDecimal::zero(),
                canceled: BigDecimal::zero(),
                status: "open".to_owned(),
                cancel_reason: None,
                reduce_only: e.reduce_only,
                expiration_timestamp_ms: e.expiration_timestamp_ms.map(integer),
                client_order_id: e.client_order_id.map(integer),
                integrator_id: e.integrator_id.map(i64::from),
                integrator_fee_rate: i64::from(e.integrator_fee_rate),
                created_checkpoint: ctx.checkpoint,
                created_at_ms: ctx.timestamp_ms,
                created_tx: ctx.tx_digest.to_owned(),
                updated_checkpoint: ctx.checkpoint,
                updated_at_ms: ctx.timestamp_ms,
            }));
            out.push(prices(
                &e.ch_id,
                Some(&e.mark_price),
                None,
                e.book_price.map(b9),
            ));
        }
        E::CanceledOrder(e) => {
            out.push(Change::OrderUpdated(OrderUpdate {
                market: id(&e.ch_id),
                order_id: integer(e.order_id.0),
                filled: BigDecimal::zero(),
                canceled: b9(e.size),
                remaining: BigDecimal::zero(),
                cancel_reason: Some(i16::from(e.cancelation_reason)),
                checkpoint: ctx.checkpoint,
                timestamp_ms: ctx.timestamp_ms,
            }));
            out.push(prices(&e.ch_id, None, None, e.book_price.map(b9)));
        }
        E::FilledMakerOrders(batch) => {
            for (fill_index, e) in batch.events.iter().enumerate() {
                let (is_ask, price) = order_side_and_price(e.order_id.0);
                out.push(Change::OrderUpdated(OrderUpdate {
                    market: id(&e.ch_id),
                    order_id: integer(e.order_id.0),
                    filled: b9(e.filled_size),
                    canceled: b9(e.canceled_size),
                    remaining: b9(e.remaining_size),
                    cancel_reason: e.cancelation_reason.map(i16::from),
                    checkpoint: ctx.checkpoint,
                    timestamp_ms: ctx.timestamp_ms,
                }));
                // An order can be consumed without trading, e.g. when it has expired.
                if e.filled_size != 0 {
                    let (size, price) = (b9(e.filled_size), b9(price));
                    out.push(Change::Fill(Fill {
                        counterparty_account_id: Some(int8(e.taker_account_id)?),
                        liquidity: "maker".to_owned(),
                        quote: Some(&size * &price),
                        price: Some(price),
                        size,
                        fee: ifixed(&e.maker_fees),
                        integrator_fee: ifixed(&e.integrator_fee_paid_usd),
                        pnl: ifixed(&e.pnl),
                        order_id: Some(integer(e.order_id.0)),
                        client_order_id: e.client_order_id.map(integer),
                        mark_price: Some(ifixed(&e.mark_price)),
                        ..fill(fill_index as i64, &e.ch_id, e.maker_account_id, is_ask)?
                    }));
                }
            }
            if let Some(first) = batch.events.first() {
                out.push(prices(
                    &first.ch_id,
                    Some(&first.mark_price),
                    None,
                    batch.book_price.map(b9),
                ));
            }
        }
        E::FilledTakerOrder(e) => {
            // A session fills on one side only; both are handled for completeness.
            let sides = [
                (true, &e.base_asset_delta_ask, &e.quote_asset_delta_ask),
                (false, &e.base_asset_delta_bid, &e.quote_asset_delta_bid),
            ];
            for (fill_index, (is_ask, base, quote)) in sides.into_iter().enumerate() {
                let (size, quote) = (ifixed(base).abs(), ifixed(quote).abs());
                if size.is_zero() {
                    continue;
                }
                out.push(Change::Fill(Fill {
                    price: price_of(&quote, &size),
                    size,
                    quote: Some(quote),
                    fee: ifixed(&e.taker_fees),
                    integrator_fee: ifixed(&e.integrator_fee_paid_usd),
                    pnl: ifixed(&e.taker_pnl),
                    mark_price: Some(ifixed(&e.mark_price)),
                    ..fill(fill_index as i64, &e.ch_id, e.taker_account_id, is_ask)?
                }));
            }
            out.push(prices(&e.ch_id, Some(&e.mark_price), None, None));
        }

        E::LiquidatedPosition(e) => {
            let (size, quote) = (
                ifixed(&e.base_liquidated).abs(),
                ifixed(&e.quote_liquidated).abs(),
            );
            out.push(Change::Fill(Fill {
                counterparty_account_id: Some(int8(e.liqor_account_id)?),
                kind: "liquidated".to_owned(),
                price: price_of(&quote, &size),
                size,
                quote: Some(quote),
                fee: ifixed(&e.liquidation_fees) + ifixed(&e.insurance_fund_fees),
                pnl: ifixed(&e.liqee_pnl),
                mark_price: Some(ifixed(&e.mark_price)),
                // Closing a long sells it.
                ..fill(0, &e.ch_id, e.liqee_account_id, e.is_liqee_long)?
            }));
            out.push(prices(&e.ch_id, Some(&e.mark_price), None, None));
        }
        E::PerformedLiquidation(e) => {
            let (size, quote) = (
                ifixed(&e.base_liquidated).abs(),
                ifixed(&e.quote_liquidated).abs(),
            );
            out.push(Change::Fill(Fill {
                counterparty_account_id: Some(int8(e.liqee_account_id)?),
                kind: "liquidation".to_owned(),
                price: price_of(&quote, &size),
                size,
                quote: Some(quote),
                // The liquidator is paid the fee.
                fee: -ifixed(&e.liqor_fees),
                pnl: ifixed(&e.liqor_pnl),
                mark_price: Some(ifixed(&e.mark_price)),
                // The liquidator takes the position over: it buys a long.
                ..fill(0, &e.ch_id, e.liqor_account_id, !e.is_liqee_long)?
            }));
        }
        E::PerformedADL(e) => {
            // The account in bad debt is closed against a counterparty on the other side, at the
            // ADL price. The event does not report either side's profit.
            let (size, price) = (b9(e.size_reduced), b9(e.adl_price));
            let sides = [
                (
                    e.bad_debt_account_id,
                    e.counterparty_account_id,
                    e.bad_debt_is_long,
                ),
                (
                    e.counterparty_account_id,
                    e.bad_debt_account_id,
                    !e.bad_debt_is_long,
                ),
            ];
            for (fill_index, (account_id, counterparty, is_ask)) in sides.into_iter().enumerate() {
                out.push(Change::Fill(Fill {
                    counterparty_account_id: Some(int8(counterparty)?),
                    kind: "adl".to_owned(),
                    price: Some(price.clone()),
                    size: size.clone(),
                    quote: Some(&size * &price),
                    ..fill(fill_index as i64, &e.ch_id, account_id, is_ask)?
                }));
            }
        }
        E::ClosedPositionAtSettlementPrices(e) => {
            // Settlement hands the position's collateral back to the account without a
            // deallocation event of its own.
            out.push(transfer(
                e.account_id,
                "settlement",
                Some(&e.ch_id),
                e.deallocated_collateral,
            )?);
            let base = ifixed(&e.base_asset_amount);
            if !base.is_zero() {
                out.push(Change::Fill(Fill {
                    kind: "settlement".to_owned(),
                    size: base.abs(),
                    pnl: ifixed(&e.pnl),
                    ..fill(0, &e.ch_id, e.account_id, base > BigDecimal::zero())?
                }));
            }
        }

        // Funding is expressed as a rate of the index price. The TWAP events sampled just before
        // carry it; when none was due, it is filled in from the market's row.
        E::UpdatedFunding(e) => {
            let market = id(&e.ch_id);
            out.push(Change::FundingUpdate(FundingUpdate {
                checkpoint: ctx.checkpoint,
                tx_index: ctx.tx_index,
                event_index: ctx.event_index,
                timestamp_ms: ctx.timestamp_ms,
                index_price: last_index_price(out, &market),
                market,
                cum_funding_rate_long: ifixed(&e.cum_funding_rate_long),
                cum_funding_rate_short: ifixed(&e.cum_funding_rate_short),
                funding_last_upd_ms: int8(e.funding_last_upd_ms)?,
            }));
        }
        E::SettledFunding(e) => {
            let market = id(&e.ch_id);
            out.push(Change::FundingPayment(FundingPayment {
                checkpoint: ctx.checkpoint,
                tx_index: ctx.tx_index,
                event_index: ctx.event_index,
                tx_digest: ctx.tx_digest.to_owned(),
                timestamp_ms: ctx.timestamp_ms,
                index_price: last_index_price(out, &market),
                market,
                account_id: int8(e.account_id)?,
                collateral_change_usd: ifixed(&e.collateral_change_usd),
                collateral_after: ifixed(&e.collateral_after),
                cum_funding_rate_long: ifixed(&e.mkt_funding_rate_long),
                cum_funding_rate_short: ifixed(&e.mkt_funding_rate_short),
                position_base: None,
            }));
        }

        // Market parameters, open interest, margin settings and the like are read from the
        // objects the transaction wrote, not from these events.
        _ => {}
    }
    Ok(())
}

fn orders_event(
    ctx: &EventContext<'_>,
    event: perpetuals_orders::Event,
    out: &mut Vec<Change>,
) -> anyhow::Result<()> {
    use perpetuals_orders::Event as E;

    let ticket = |ticket_id: &Address, kind: &str, account_id: u64| {
        anyhow::Ok(OrderTicket {
            ticket_id: id(ticket_id),
            kind: kind.to_owned(),
            account_id: int8(account_id)?,
            collateral_type: ctx.collateral_type.clone().unwrap_or_default(),
            market: None,
            status: "open".to_owned(),
            executors: json!([]),
            execution_domain: None,
            gas: BigDecimal::zero(),
            stop_order_type: None,
            encrypted_details: vec![],
            twap_progress: None,
            created_checkpoint: ctx.checkpoint,
            created_at_ms: ctx.timestamp_ms,
            updated_checkpoint: ctx.checkpoint,
            updated_at_ms: ctx.timestamp_ms,
        })
    };
    let status = |ticket_id: &Address, status: &'static str| {
        Change::Ticket(TicketChange::Status {
            ticket_id: id(ticket_id),
            status,
            checkpoint: ctx.checkpoint,
            timestamp_ms: ctx.timestamp_ms,
        })
    };
    let details = |ticket_id: &Address, encrypted_details: Vec<u8>| {
        Change::Ticket(TicketChange::Details {
            ticket_id: id(ticket_id),
            encrypted_details,
            checkpoint: ctx.checkpoint,
            timestamp_ms: ctx.timestamp_ms,
        })
    };
    let executors = |ticket_id: &Address, executors: &[Address]| {
        Change::Ticket(TicketChange::Executors {
            ticket_id: id(ticket_id),
            executors: json!(executors),
            checkpoint: ctx.checkpoint,
            timestamp_ms: ctx.timestamp_ms,
        })
    };

    out.push(match event {
        E::CreatedStopOrderTicket(e) => {
            Change::Ticket(TicketChange::Created(Box::new(OrderTicket {
                executors: json!(e.executors),
                execution_domain: e.execution_domain.as_ref().map(id),
                gas: integer(e.gas),
                stop_order_type: Some(integer(e.stop_order_type)),
                encrypted_details: e.encrypted_details.0,
                ..ticket(&e.ticket_id, "stop", e.account_id)?
            })))
        }
        E::ExecutedStopOrderTicket(e) => status(&e.ticket_id, "executed"),
        E::DeletedStopOrderTicket(e) => status(&e.ticket_id, "deleted"),
        E::EditedStopOrderTicketDetails(e) => details(&e.ticket_id, e.encrypted_details.0),
        E::EditedStopOrderTicketExecutors(e) => executors(&e.ticket_id, &e.executors),

        E::CreatedTWAPOrderTicket(e) => {
            Change::Ticket(TicketChange::Created(Box::new(OrderTicket {
                market: Some(id(&e.ch_id)),
                executors: json!(e.executors),
                execution_domain: e.execution_domain.as_ref().map(id),
                gas: integer(e.gas),
                encrypted_details: e.encrypted_details.0,
                ..ticket(&e.ticket_id, "twap", e.account_id)?
            })))
        }
        E::ProcessedTWAPOrderTicket(e) => Change::Ticket(TicketChange::Progress {
            ticket_id: id(&e.ticket_id),
            progress: serde_json::to_value(&e)?,
            checkpoint: ctx.checkpoint,
            timestamp_ms: ctx.timestamp_ms,
        }),
        E::FinalizedTWAPOrderTicket(e) => status(&e.ticket_id, "finalized"),
        E::CanceledTWAPOrderTicket(e) => status(&e.ticket_id, "canceled"),
        E::DeletedTWAPOrderTicket(e) => status(&e.ticket_id, "deleted"),
        E::EditedTWAPOrderTicketDetails(e) => details(&e.ticket_id, e.encrypted_details.0),
        E::EditedTWAPOrderTicketExecutors(e) => executors(&e.ticket_id, &e.executors),
    });
    Ok(())
}

fn oracle_event(
    ctx: &EventContext<'_>,
    event: oracle_aggregator::Event,
    out: &mut Vec<Change>,
) -> anyhow::Result<()> {
    use oracle_aggregator::Event as E;

    match event {
        // A feed starts with its spot price as its TWAP.
        E::CreatedPriceFeed(e) => out.push(Change::OraclePrice(OraclePrice {
            storage_id: i64::from(e.storage_id),
            source_id: i32::from(e.source_id),
            price: oracle_price(e.price),
            twap_price: oracle_price(e.price),
            timestamp_ms: int8(e.timestamp_ms)?,
            updated_checkpoint: ctx.checkpoint,
        })),
        E::UpdatedPriceFeed(e) => out.push(Change::OraclePrice(OraclePrice {
            storage_id: i64::from(e.storage_id),
            source_id: i32::from(e.source_id),
            price: oracle_price(e.new_price),
            twap_price: oracle_price(e.new_twap_price),
            timestamp_ms: int8(e.new_timestamp_ms)?,
            updated_checkpoint: ctx.checkpoint,
        })),
        E::RemovedPriceFeed(e) => out.push(Change::OraclePriceRemoved {
            storage_id: i64::from(e.storage_id),
            source_id: i32::from(e.source_id),
        }),
        _ => {}
    }
    Ok(())
}

/// Whether `tag` is `<perpetuals>::<module>::<name>`, for any indexed version of the package.
fn is_perpetuals_type(tag: &StructTag, packages: &Packages, module: &str, name: &str) -> bool {
    tag.module.as_str() == module
        && tag.name.as_str() == name
        && packages
            .get(&tag.address)
            .is_some_and(|package| package.decoder == Some(Package::Perpetuals))
}

/// The role of an account capability, `AuthorityCap<perpetuals::authority::ACCOUNT, Role>`, or
/// `None` if `tag` is any other type. The capability type itself lives in a package of its own,
/// so it is recognized by what it is a capability over.
fn account_cap_role(tag: &StructTag, packages: &Packages) -> Option<String> {
    if tag.module.as_str() != "authority" || tag.name.as_str() != "AuthorityCap" {
        return None;
    }
    let [TypeTag::Struct(context), TypeTag::Struct(role)] = tag.type_params.as_slice() else {
        return None;
    };
    is_perpetuals_type(context, packages, "authority", "ACCOUNT")
        .then(|| role.name.as_str().to_ascii_lowercase())
}

/// Snapshots `object` if it is one the state is read from: a clearing house, an account, a
/// capability over an account, or a position (a dynamic field of its clearing house).
fn object_snapshot(
    checkpoint: i64,
    timestamp_ms: i64,
    object: &Object,
    packages: &Packages,
    out: &mut Vec<Change>,
) -> anyhow::Result<()> {
    let (Some(tag), Some(contents)) = (object.struct_tag(), object.data.try_as_move()) else {
        return Ok(());
    };
    let contents = contents.contents();
    let first_type_param = || tag.type_params.first().map(type_name).unwrap_or_default();

    if is_perpetuals_type(&tag, packages, "clearing_house", "ClearingHouse") {
        let ch: ClearingHouse = bcs::from_bytes(contents).context("decoding ClearingHouse")?;
        let (params, state) = (&ch.market_params, &ch.market_state);
        let core = &params.core_params;
        out.push(Change::MarketSnapshot(Box::new(MarketSnapshot {
            market: id(&ch.id),
            collateral_type: first_type_param(),
            version: int8(ch.version)?,
            paused: i16::from(ch.paused),
            base_storage_id: i64::from(core.base_storage_id),
            base_source_id: i32::from(core.base_source_id),
            collateral_storage_id: i64::from(core.collateral_storage_id),
            collateral_source_id: i32::from(core.collateral_source_id),
            lot_size: b9(core.lot_size),
            tick_size: b9(core.tick_size),
            margin_ratio_initial: ifixed(&core.margin_ratio_initial),
            margin_ratio_maintenance: ifixed(&core.margin_ratio_maintenance),
            maker_fee: ifixed(&params.fees_params.maker_fee),
            taker_fee: ifixed(&params.fees_params.taker_fee),
            params: serde_json::to_value(params)?,
            cum_funding_rate_long: ifixed(&state.cum_funding_rate_long),
            cum_funding_rate_short: ifixed(&state.cum_funding_rate_short),
            funding_last_upd_ms: int8(state.funding_last_upd_ms)?,
            premium_twap: ifixed(&state.premium_twap),
            premium_twap_last_upd_ms: int8(state.premium_twap_last_upd_ms)?,
            spread_twap: ifixed(&state.spread_twap),
            spread_twap_last_upd_ms: int8(state.spread_twap_last_upd_ms)?,
            open_interest: ifixed(&state.open_interest),
            fees_accrued: ifixed(&state.fees_accrued),
            order_counter: integer(ch.orderbook.counter),
            best_ask_price: ch.orderbook.best_ask_price.map(b9),
            best_bid_price: ch.orderbook.best_bid_price.map(b9),
            updated_checkpoint: checkpoint,
            updated_at_ms: timestamp_ms,
        })));
    } else if is_perpetuals_type(&tag, packages, "account", "Account") {
        let account: Account = bcs::from_bytes(contents).context("decoding Account")?;
        out.push(Change::AccountSnapshot(AccountSnapshot {
            account_id: int8(account.account_id)?,
            object_id: id(&account.id),
            collateral_type: first_type_param(),
            collateral: integer(account.collateral),
            updated_checkpoint: checkpoint,
            updated_at_ms: timestamp_ms,
        }));
    } else if let Some(role) = account_cap_role(&tag, packages) {
        let cap: AuthorityCap = bcs::from_bytes(contents).context("decoding AuthorityCap")?;
        let owner = match object.owner() {
            Owner::AddressOwner(owner) => {
                Some(AccountAddress::from(*owner).to_canonical_string(/* with_prefix */ true))
            }
            _ => None,
        };
        out.push(Change::AccountCap(AccountCap {
            cap_id: id(&cap.id),
            account_object_id: id(&cap.r#for),
            role,
            owner,
            updated_checkpoint: checkpoint,
        }));
    } else if tag.address == AccountAddress::TWO
        && tag.module.as_str() == "dynamic_field"
        && tag.name.as_str() == "Field"
        && matches!(
            tag.type_params.first(),
            Some(TypeTag::Struct(key)) if is_perpetuals_type(key, packages, "keys", "PositionKey")
        )
    {
        let Owner::ObjectOwner(market) = object.owner() else {
            anyhow::bail!("position field is not owned by an object");
        };
        let field: PositionField = bcs::from_bytes(contents).context("decoding Position")?;
        let position = &field.value;
        out.push(Change::PositionSnapshot(PositionSnapshot {
            market: AccountAddress::from(*market).to_canonical_string(/* with_prefix */ true),
            account_id: int8(field.name.account_id)?,
            object_id: id(&field.id),
            collateral: ifixed(&position.collateral),
            base: ifixed(&position.base_asset_amount),
            quote_notional: ifixed(&position.quote_asset_notional_amount),
            cum_funding_rate_long: ifixed(&position.cum_funding_rate_long),
            cum_funding_rate_short: ifixed(&position.cum_funding_rate_short),
            asks_quantity: ifixed(&position.asks_quantity),
            bids_quantity: ifixed(&position.bids_quantity),
            pending_orders: int8(position.pending_orders)?,
            initial_margin_ratio: ifixed(&position.initial_margin_ratio),
            updated_checkpoint: checkpoint,
            updated_at_ms: timestamp_ms,
        }));
    }
    Ok(())
}
