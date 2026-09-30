// Copyright (c) 2026 Geunhwa Jeong
// SPDX-License-Identifier: Apache-2.0

//! Events of `perpetuals_orders::events`: stop order and TWAP order tickets.

#![allow(clippy::upper_case_acronyms)]

use crate::types::*;

move_events! {
    struct CreatedStopOrderTicket {
        ticket_id: Id,
        account_id: u64,
        executors: Vec<Address>,
        execution_domain: Option<Address>,
        gas: u64,
        stop_order_type: u64,
        encrypted_details: Bytes,
    }

    struct ExecutedStopOrderTicket {
        ticket_id: Id,
        account_id: u64,
        executor: Address,
    }

    struct DeletedStopOrderTicket {
        ticket_id: Id,
        account_id: u64,
        executor: Address,
    }

    struct EditedStopOrderTicketDetails {
        ticket_id: Id,
        account_id: u64,
        encrypted_details: Bytes,
    }

    struct EditedStopOrderTicketExecutors {
        ticket_id: Id,
        account_id: u64,
        executors: Vec<Address>,
    }

    struct CreatedTWAPOrderTicket {
        ticket_id: Id,
        ch_id: Id,
        account_id: u64,
        executors: Vec<Address>,
        execution_domain: Option<Address>,
        gas: u64,
        encrypted_details: Bytes,
    }

    struct ProcessedTWAPOrderTicket {
        ticket_id: Id,
        account_id: u64,
        execution_amount: u64,
        filled_amount: u64,
        remainder: u64,
        processed_amount: u64,
        scheduled_amount: u64,
        last_attempt_timestamp_ms: u64,
        retry_anchor_timestamp_ms: u64,
        last_execution_timestamp_ms: u64,
    }

    struct FinalizedTWAPOrderTicket {
        ticket_id: Id,
        account_id: u64,
        executor: Address,
        deallocated_collateral: u64,
    }

    struct CanceledTWAPOrderTicket {
        ticket_id: Id,
        account_id: u64,
        sender: Address,
        deallocated_collateral: u64,
        partial_fill: bool,
    }

    struct DeletedTWAPOrderTicket {
        ticket_id: Id,
        account_id: u64,
        executor: Address,
    }

    struct EditedTWAPOrderTicketDetails {
        ticket_id: Id,
        account_id: u64,
        encrypted_details: Bytes,
    }

    struct EditedTWAPOrderTicketExecutors {
        ticket_id: Id,
        account_id: u64,
        executors: Vec<Address>,
    }
}
