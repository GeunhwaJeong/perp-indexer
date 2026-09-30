// Copyright (c) 2026 Geunhwa Jeong
// SPDX-License-Identifier: Apache-2.0

//! The packages whose events are indexed.
//!
//! Package addresses differ per network and grow with every upgrade, so they are configuration
//! rather than constants. An event type keeps the address of the package version that first
//! defined it: after an upgrade that adds events, list the new version's address as well.

use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;

use move_core_types::account_address::AccountAddress;
use perp_types::Package;

/// A `<name>=<address>` command line argument.
#[derive(Clone, Debug)]
pub struct PackageArg {
    pub name: String,
    pub address: AccountAddress,
}

impl FromStr for PackageArg {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (name, address) = s
            .split_once('=')
            .ok_or_else(|| format!("expected <name>=<address>, got '{s}'"))?;
        if name.is_empty() {
            return Err(format!("missing package name in '{s}'"));
        }
        let address = AccountAddress::from_hex_literal(address)
            .map_err(|e| format!("invalid package address '{address}': {e}"))?;
        Ok(Self {
            name: name.to_owned(),
            address,
        })
    }
}

/// An indexed package, as seen by the handlers.
#[derive(Clone, Debug)]
pub struct IndexedPackage {
    pub name: Arc<str>,
    /// `None` for packages that are recorded without being decoded.
    pub decoder: Option<Package>,
}

/// Lookup from the address in an event type to the package it belongs to.
#[derive(Clone, Debug, Default)]
pub struct Packages(HashMap<AccountAddress, IndexedPackage>);

impl Packages {
    pub fn new(args: impl IntoIterator<Item = PackageArg>) -> anyhow::Result<Self> {
        let mut packages = HashMap::new();
        for PackageArg { name, address } in args {
            let package = IndexedPackage {
                decoder: name.parse().ok(),
                name: name.into(),
            };
            if let Some(previous) = packages.insert(address, package) {
                anyhow::bail!(
                    "package address {address} is listed twice ('{}')",
                    previous.name
                );
            }
        }
        anyhow::ensure!(!packages.is_empty(), "no packages to index");
        Ok(Self(packages))
    }

    pub fn get(&self, address: &AccountAddress) -> Option<&IndexedPackage> {
        self.0.get(address)
    }

    pub fn iter(&self) -> impl Iterator<Item = (&AccountAddress, &IndexedPackage)> {
        self.0.iter()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_name_and_address() {
        let arg: PackageArg = "perpetuals=0x2".parse().unwrap();
        assert_eq!(arg.name, "perpetuals");
        assert_eq!(arg.address, AccountAddress::TWO);
        assert!("perpetuals".parse::<PackageArg>().is_err());
        assert!("=0x2".parse::<PackageArg>().is_err());
        assert!("perpetuals=zz".parse::<PackageArg>().is_err());
    }

    #[test]
    fn known_names_get_decoders_and_versions_share_them() {
        let packages = Packages::new([
            "perpetuals=0x2".parse().unwrap(),
            "perpetuals=0x3".parse().unwrap(),
            "market_making_vault=0x4".parse().unwrap(),
        ])
        .unwrap();
        let address = |n: u8| AccountAddress::from_hex_literal(&format!("0x{n}")).unwrap();
        assert_eq!(
            packages.get(&address(2)).unwrap().decoder,
            Some(Package::Perpetuals)
        );
        assert_eq!(
            packages.get(&address(3)).unwrap().decoder,
            Some(Package::Perpetuals)
        );
        assert_eq!(packages.get(&address(4)).unwrap().decoder, None);
        assert!(packages.get(&address(5)).is_none());
    }

    #[test]
    fn rejects_duplicates_and_empty_sets() {
        assert!(Packages::new([]).is_err());
        let duplicate = [
            "perpetuals=0x2".parse().unwrap(),
            "oracle_aggregator=0x2".parse().unwrap(),
        ];
        assert!(Packages::new(duplicate).is_err());
    }
}
