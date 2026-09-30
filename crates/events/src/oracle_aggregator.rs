// Copyright (c) 2026 Geunhwa Jeong
// SPDX-License-Identifier: Apache-2.0

//! Events of `oracle_aggregator::events`: price feed storages, sources and price updates.

#![allow(clippy::upper_case_acronyms)]

use crate::types::*;

move_events! {
    struct CreatedPriceFeedStorage {
        price_feed_storage_obj_id: Id,
        storage_id: u32,
        symbol: String,
    }

    struct CreatedSource {
        source_id: u16,
        source_object_id: Id,
    }

    struct UpgradedSourceVersion {
        source_id: u16,
        version: u64,
    }

    struct AddedAuthorization {
        source_id: u16,
    }

    struct RemovedAuthorization {
        source_id: u16,
    }

    struct CreatedPriceFeed {
        storage_id: u32,
        source_id: u16,
        price: U128,
        timestamp_ms: u64,
    }

    struct RemovedPriceFeed {
        storage_id: u32,
        source_id: u16,
    }

    struct UpdatedPriceFeed {
        storage_id: u32,
        source_id: u16,
        old_price: U128,
        old_timestamp_ms: u64,
        old_twap_price: U128,
        new_price: U128,
        new_timestamp_ms: u64,
        new_twap_price: U128,
    }

    struct UpdatedTwapPeriodMs {
        storage_id: u32,
        source_id: u16,
        old_twap_period_ms: u64,
        new_twap_period_ms: u64,
    }

    struct SetVendorRegistration {
        open: bool,
    }

    struct RegisteredVendor {
        vendor_key: TypeName,
        vendor_admin_cap_id: Id,
    }

    struct CreatedPackageRevokeVendorGuardianCap {
        cap_id: Id,
    }

    struct GuardianRevokedVendorAuthorityCap {
        vendor_key: TypeName,
        role: TypeName,
        cap_id: Id,
    }

    struct ReauthorizedVendorAdminCap {
        vendor_key: TypeName,
        cap_id: Id,
    }

    struct CreatedPackageFreezeGuardianCap {
        cap_id: Id,
    }

    struct Froze {
        id: Id,
        resume_version: u64,
        guardian_cap_id: Id,
    }

    struct Unfroze {
        id: Id,
        version: u64,
    }
}
