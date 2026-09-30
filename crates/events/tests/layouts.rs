// Copyright (c) 2026 Geunhwa Jeong
// SPDX-License-Identifier: Apache-2.0

//! Checks the Rust event declarations against the layouts extracted from the engine's Move
//! sources (`scripts/extract_event_layouts.py`). BCS decoding is positional, so a field that is
//! missing, reordered or mistyped would silently shift every value after it.

use perp_events::Package;

fn layout_file(package: Package) -> &'static str {
    match package {
        Package::Perpetuals => include_str!("../layouts/perpetuals.layout"),
        Package::PerpetualsOrders => include_str!("../layouts/perpetuals_orders.layout"),
        Package::OracleAggregator => include_str!("../layouts/oracle_aggregator.layout"),
    }
}

/// The Rust type a Move field type must be declared as.
fn rust_type(move_type: &str) -> String {
    let move_type = move_type.trim();
    if let Some(inner) = move_type
        .strip_prefix("Option<")
        .and_then(|t| t.strip_suffix('>'))
    {
        return format!("Option<{}>", rust_type(inner));
    }
    if let Some(inner) = move_type
        .strip_prefix("vector<")
        .and_then(|t| t.strip_suffix('>'))
    {
        return match inner.trim() {
            "u8" => "Bytes".to_owned(),
            inner => format!("Vec<{}>", rust_type(inner)),
        };
    }
    match move_type {
        "u128" => "U128",
        "u256" => "U256",
        "ID" => "Id",
        "address" => "Address",
        other => other,
    }
    .to_owned()
}

fn expected(package: Package) -> Vec<(String, Vec<(String, String)>)> {
    let mut events: Vec<(String, Vec<(String, String)>)> = vec![];
    for line in layout_file(package).lines() {
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        if let Some(field) = line.strip_prefix("  ") {
            let (name, ty) = field.split_once(':').expect("field line is `name: type`");
            let fields = &mut events.last_mut().expect("field follows a struct").1;
            fields.push((name.trim().to_owned(), rust_type(ty)));
        } else {
            events.push((line.to_owned(), vec![]));
        }
    }
    events
}

fn declared(package: Package) -> Vec<(String, Vec<(String, String)>)> {
    package
        .layout()
        .iter()
        .map(|(name, fields)| {
            let fields = fields
                .iter()
                .map(|(field, ty)| (field.to_string(), ty.replace(' ', "")))
                .collect();
            (name.to_string(), fields)
        })
        .collect()
}

#[test]
fn declarations_match_the_move_sources() {
    for package in Package::ALL {
        let expected = expected(package);
        assert!(!expected.is_empty(), "{package}: empty layout file");
        assert_eq!(
            declared(package),
            expected,
            "{package}: event layouts drifted"
        );
    }
}
