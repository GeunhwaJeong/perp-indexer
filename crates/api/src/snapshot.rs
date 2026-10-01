// Copyright (c) 2026 Geunhwa Jeong
// SPDX-License-Identifier: Apache-2.0

//! What an account looks like at one checkpoint, read from the database in one snapshot.

use bigdecimal::BigDecimal;
use haneul_pg_db::{Connection, Db};

use crate::config::Deployment;
use crate::db::{self, AccountRow, OrderFilter, Watermark};
use crate::hub::{AccountUpdate, ChildUpdate};
use crate::model::{Order, ParentSubaccount};
use crate::views::{
    self, Child, MarketView, MarketViews, account_asset, account_balance, order_object,
};

/// Open orders sent with an account's initial data. The engine caps the orders a position may
/// have pending, so this is only reached by an account trading a great many markets.
const MAX_OPEN_ORDERS: i64 = 1_000;

/// The listed markets that can be served, priced as of `now_ms`.
pub async fn market_views(
    conn: &mut Connection<'_>,
    deployment: &Deployment,
    now_ms: i64,
) -> anyhow::Result<MarketViews> {
    let rows = db::markets(conn, &deployment.market_ids()).await?;
    Ok(rows
        .into_iter()
        .filter_map(|row| {
            let ticker = deployment.ticker(&row.market)?;
            let view = MarketView::new(row.clone(), ticker, now_ms)?;
            Some((row.market, view))
        })
        .collect())
}

/// An account with everything a subscriber starts from.
#[derive(Clone, Debug)]
pub struct AccountState {
    pub row: AccountRow,
    /// What the account's unallocated balance is worth in USD.
    pub balance: BigDecimal,
    pub children: Vec<Child>,
    pub open_orders: Vec<Order>,
}

impl AccountState {
    pub fn subaccount(&self, address: &str, parent: i64, checkpoint: i64) -> ParentSubaccount {
        views::parent_subaccount(
            address,
            parent,
            &self.balance,
            &self.row,
            &self.children,
            checkpoint,
        )
    }

    /// The whole state as an update, for a subscriber that was told the account did not exist.
    pub fn as_update(&self) -> AccountUpdate {
        let mut update = AccountUpdate::default();
        update.children.entry(0).or_default().asset_positions =
            vec![account_asset(&self.balance, 0)];
        for child in &self.children {
            let entry: &mut ChildUpdate =
                update.children.entry(child.subaccount_number).or_default();
            entry.asset_positions.push(child.asset.clone());
            entry.perpetual_positions.extend(
                child
                    .position
                    .iter()
                    .filter(|p| p.status == "OPEN")
                    .cloned(),
            );
        }
        for order in &self.open_orders {
            update
                .children
                .entry(order.subaccount_number)
                .or_default()
                .orders
                .push(order.clone());
        }
        update
    }
}

/// The account an address trades through, as of `watermark`. `account` is `None` when the
/// address holds no account.
#[derive(Clone, Debug)]
pub struct AccountSnapshot {
    pub watermark: Watermark,
    pub account: Option<AccountState>,
}

/// Loads the account that `address` holds its `parent`-th admin capability for.
///
/// Subaccount numbers in the result are those of parent subaccount 0; `parent` only selects the
/// account.
pub async fn account(
    db: &Db,
    deployment: &Deployment,
    address: &str,
    parent: i64,
) -> anyhow::Result<AccountSnapshot> {
    let mut snapshot = db::snapshot(db).await?;
    let conn = snapshot.conn();
    let watermark = db::watermark(conn)
        .await?
        .ok_or_else(|| anyhow::anyhow!("the indexer has not committed anything yet"))?;
    let Some(row) = db::account_of(conn, address, &deployment.collateral_type, parent).await?
    else {
        snapshot.finish().await?;
        return Ok(AccountSnapshot {
            watermark,
            account: None,
        });
    };

    let market_ids = deployment.market_ids();
    let markets = market_views(conn, deployment, watermark.timestamp_ms).await?;
    let children = db::account_positions(conn, &market_ids, row.account_id)
        .await?
        .iter()
        .filter_map(|position| Some(Child::new(position, markets.get(&position.market)?, 0)))
        .collect();
    let filter = OrderFilter {
        status: Some("open".to_owned()),
        limit: MAX_OPEN_ORDERS,
        ..OrderFilter::default()
    };
    let open_orders = db::account_orders(conn, &market_ids, row.account_id, &filter)
        .await?
        .iter()
        .filter_map(|order| Some(order_object(order, deployment.ticker(&order.market)?, 0)))
        .collect();
    snapshot.finish().await?;

    let balance = account_balance(
        &row,
        deployment.collateral_decimals,
        &views::collateral_price(&markets),
    );
    Ok(AccountSnapshot {
        watermark,
        account: Some(AccountState {
            row,
            balance,
            children,
            open_orders,
        }),
    })
}
