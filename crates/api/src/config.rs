// Copyright (c) 2026 Geunhwa Jeong
// SPDX-License-Identifier: Apache-2.0

//! What the API serves: the markets it lists, under which tickers, and the collateral they
//! trade in. The file is the deployment description the front end already reads, so both sides
//! take their market list from one place.

use std::collections::{BTreeMap, HashMap};
use std::path::Path;

use anyhow::{Context, bail, ensure};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct DeploymentFile {
    collateral: CollateralFile,
    markets: BTreeMap<String, MarketFile>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CollateralFile {
    coin_type: String,
    decimals: u32,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct MarketFile {
    clearing_house: String,
}

/// The markets and collateral the API serves.
#[derive(Clone, Debug)]
pub struct Deployment {
    /// The collateral coin type, in the canonical form the indexer stores.
    pub collateral_type: String,
    pub collateral_decimals: u32,
    /// Ticker by clearing house ID.
    tickers: HashMap<String, String>,
    /// Clearing house ID by ticker.
    markets: BTreeMap<String, String>,
}

impl Deployment {
    pub fn load(path: &Path) -> anyhow::Result<Self> {
        let contents = std::fs::read_to_string(path)
            .with_context(|| format!("reading deployment file {}", path.display()))?;
        Self::parse(&contents).with_context(|| format!("deployment file {}", path.display()))
    }

    pub fn parse(json: &str) -> anyhow::Result<Self> {
        let file: DeploymentFile = serde_json::from_str(json)?;
        ensure!(!file.markets.is_empty(), "no markets listed");

        let (address, rest) = file
            .collateral
            .coin_type
            .split_once("::")
            .context("collateral coin type is not <address>::<module>::<name>")?;
        let collateral_type = format!("{}::{rest}", canonical_address(address)?);

        let mut tickers = HashMap::new();
        let mut markets = BTreeMap::new();
        for (ticker, market) in file.markets {
            ensure!(
                !ticker.is_empty() && !ticker.contains('/'),
                "invalid ticker '{ticker}'"
            );
            let id = canonical_address(&market.clearing_house)
                .with_context(|| format!("clearing house of {ticker}"))?;
            if let Some(other) = tickers.insert(id.clone(), ticker.clone()) {
                bail!("{ticker} and {other} name the same clearing house");
            }
            markets.insert(ticker, id);
        }

        Ok(Self {
            collateral_type,
            collateral_decimals: file.collateral.decimals,
            tickers,
            markets,
        })
    }

    pub fn ticker(&self, market: &str) -> Option<&str> {
        self.tickers.get(market).map(String::as_str)
    }

    pub fn market(&self, ticker: &str) -> Option<&str> {
        self.markets.get(ticker).map(String::as_str)
    }

    /// `(ticker, clearing house ID)` in ticker order.
    pub fn markets(&self) -> impl Iterator<Item = (&str, &str)> {
        self.markets.iter().map(|(t, m)| (t.as_str(), m.as_str()))
    }

    /// Every clearing house ID.
    pub fn market_ids(&self) -> Vec<String> {
        self.markets.values().cloned().collect()
    }
}

/// An address or object ID as the indexer stores it: `0x` and 64 lowercase hex digits.
pub fn canonical_address(address: &str) -> anyhow::Result<String> {
    let hex = address
        .strip_prefix("0x")
        .or_else(|| address.strip_prefix("0X"))
        .unwrap_or(address);
    ensure!(
        !hex.is_empty() && hex.len() <= 64 && hex.bytes().all(|b| b.is_ascii_hexdigit()),
        "'{address}' is not an address"
    );
    Ok(format!("0x{:0>64}", hex.to_ascii_lowercase()))
}

#[cfg(test)]
mod tests {
    use super::*;

    const FILE: &str = r#"{
        "network": "localnet",
        "packages": {"perpetuals": "0xc8"},
        "collateral": {"coinType": "0x7F::tusd::TUSD", "decimals": 6, "priceFeedStorage": "0x1"},
        "markets": {
            "BTC-USD": {"marketId": "BTC-USD", "clearingHouse": "0xC0b6", "lotSize": "1000000"},
            "ETH-USD": {"clearingHouse": "0x0e"}
        }
    }"#;

    #[test]
    fn reads_the_front_end_deployment_file() {
        let deployment = Deployment::parse(FILE).unwrap();
        let btc = format!("0x{:0>64}", "c0b6");
        assert_eq!(deployment.market("BTC-USD"), Some(btc.as_str()));
        assert_eq!(deployment.ticker(&btc), Some("BTC-USD"));
        assert_eq!(deployment.market("DOGE-USD"), None);
        assert_eq!(
            deployment.collateral_type,
            format!("0x{:0>64}::tusd::TUSD", "7f")
        );
        assert_eq!(deployment.collateral_decimals, 6);
        assert_eq!(deployment.market_ids().len(), 2);
    }

    #[test]
    fn rejects_files_it_cannot_serve() {
        assert!(
            Deployment::parse(
                r#"{"collateral": {"coinType": "0x1::a::A", "decimals": 6}, "markets": {}}"#
            )
            .is_err()
        );
        let duplicate = FILE.replace("0x0e", "0xc0b6");
        assert!(Deployment::parse(&duplicate).is_err());
        assert!(canonical_address("0xzz").is_err());
        assert!(canonical_address("").is_err());
        assert!(canonical_address(&"1".repeat(65)).is_err());
    }
}
