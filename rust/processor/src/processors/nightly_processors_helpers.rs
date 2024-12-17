use super::nightly_processor::{
    Nudge as IndexerNudge, TransactionChanges, TransactionTokenChanges,
};
use crate::{
    db::postgres::models::{
        fungible_asset_models::{
            v2_fungible_asset_activities::FungibleAssetActivity,
            v2_fungible_asset_balances::FungibleAssetBalance, v2_fungible_asset_utils::CoinAction,
        },
        token_v2_models::{
            v2_token_activities::{TokenAction, TokenActivityV2},
            v2_token_utils::TokenStandard,
        },
    },
    utils::counters::PROCESSOR_UNKNOWN_TYPE_COUNT,
};
use ahash::AHashMap;
use bigdecimal::{BigDecimal, ToPrimitive, Zero};
use odin::structs::{
    notifications::aptos_notifications::{
        AptosIndexerNotification, CoinFrozen, CoinReceived, CoinSent, CoinSwap, NftBurned,
        NftCancelClaim, NftClaim, NftMinted, NftOffer, NftReceived, NftSent, Nudge,
    },
    ws::{
        aptos_ws::{
            AptosAccountTokensUpdate, AptosCoinBalanceUpdate, AptosCoinObjectUpdateStatus,
            AptosCoinStandard, AptosCoinUpdate, AptosObjectUpdateStatus, AptosTokenChangeUpdate,
            AptosTokenStandard, AptosWsApiMsg, Offer, PendingClaim,
        },
        ws_message::{CoinCreated, CoinDeleted, CoinMutated, Received, Sent},
    },
};
use rayon::iter::{IntoParallelRefIterator, ParallelIterator};
use serde::{Deserialize, Serialize};
use std::{cmp::Ordering, collections::HashMap};
use tracing::warn;

// Helper struct to catch transfer between two users for v1 as direct transfer event data sucks
#[derive(Debug, Serialize, Deserialize)]
struct TokenEventData {
    from_address: Option<String>,
    to_address: Option<String>,
    token_data_id: String,
    event_index: i64,
}

pub fn process_changes(
    transactions: Vec<TransactionChanges>,
) -> (
    Vec<(u64, Vec<AptosWsApiMsg>)>,
    Vec<(u64, Vec<AptosIndexerNotification>)>,
) {
    // Process transactions in parallel and collect results
    let processed_results: Vec<(u64, Vec<AptosWsApiMsg>, Vec<AptosIndexerNotification>)> =
        transactions
            .par_iter()
            .map(|tx| {
                let tx_version = tx.txn_version;
                let coin_changes = &tx.coin_changes;
                let token_changes = &tx.token_changes;

                let mut ws_transaction_account_coin_updates: AHashMap<
                    String,
                    AptosCoinBalanceUpdate,
                > = AHashMap::new();
                let mut ws_account_token_updates: AHashMap<String, AptosAccountTokensUpdate> =
                    AHashMap::new();
                let mut transaction_notifications = Vec::new();

                // Process gas event
                let (gas_type, gas_amount, gas_payee) =
                    if let Some(gas_event) = coin_changes.gas_event.clone() {
                        process_gas_event(
                            tx_version,
                            gas_event,
                            &mut ws_transaction_account_coin_updates,
                        )
                    } else {
                        if !coin_changes.asset_balances.is_empty()
                            && !coin_changes.asset_activities.is_empty()
                        {
                            warn!(
                                "Missing gas event for transaction {txn_version}",
                                txn_version = tx_version
                            );
                        }
                        return (tx_version as u64, Vec::new(), Vec::new());
                    };

                // Process coin changes
                process_coin_changes(
                    &coin_changes.asset_activities,
                    &coin_changes.asset_balances,
                    &mut ws_transaction_account_coin_updates,
                    tx_version,
                );

                // Process token changes
                process_token_changes(token_changes, &mut ws_account_token_updates, tx_version);

                // Generate notifications
                generate_notifications(
                    &ws_transaction_account_coin_updates,
                    &ws_account_token_updates,
                    &mut transaction_notifications,
                    &gas_type,
                    gas_amount,
                    &gas_payee,
                    &tx.nudge_events,
                );

                // Convert to final format
                let ws_updates = ws_transaction_account_coin_updates
                    .into_iter()
                    .map(|(_, v)| AptosWsApiMsg::AptosCoinBalanceUpdate(v))
                    .chain(
                        ws_account_token_updates
                            .into_iter()
                            .map(|(_, v)| AptosWsApiMsg::AptosAccountTokensUpdate(v)),
                    )
                    .collect();

                (tx_version as u64, ws_updates, transaction_notifications)
            })
            .collect();

    // Combine results
    let mut ws_updates = Vec::new();
    let mut notifications = Vec::new();

    for (version, updates, notifs) in processed_results {
        ws_updates.push((version, updates));
        notifications.push((version, notifs));
    }

    (ws_updates, notifications)
}

// Helper function to process gas event
fn process_gas_event(
    tx_version: i64,
    gas_event: FungibleAssetActivity,
    ws_updates: &mut AHashMap<String, AptosCoinBalanceUpdate>,
) -> (String, i128, String) {
    let coin_type = match gas_event.asset_type.clone() {
        Some(asset_type) => asset_type,
        None => {
            tracing::warn!(
                transaction_version = tx_version,
                "Missing coin type or owner address"
            );
            PROCESSOR_UNKNOWN_TYPE_COUNT
                .with_label_values(&["NightlyProcessor"])
                .inc();
            return (String::new(), 0, String::new());
        },
    };

    let owner_address = match gas_event.gas_fee_payer_address.clone() {
        Some(fee_payer) => fee_payer,
        None => match gas_event.owner_address {
            Some(owner_address) => owner_address,
            None => {
                tracing::warn!(
                    transaction_version = tx_version,
                    "Missing coin type or owner address"
                );
                PROCESSOR_UNKNOWN_TYPE_COUNT
                    .with_label_values(&["NightlyProcessor"])
                    .inc();
                return (coin_type, 0, String::new());
            },
        },
    };

    let gas_amount = match gas_event.amount.clone().and_then(|amount| amount.to_i128()) {
        Some(amount) => amount,
        None => {
            warn!(
                "Invalid or missing gas amount for transaction {txn_version}",
                txn_version = tx_version
            );
            PROCESSOR_UNKNOWN_TYPE_COUNT
                .with_label_values(&["NightlyProcessor"])
                .inc();
            return (coin_type, 0, owner_address);
        },
    };

    ws_updates
        .entry(owner_address.clone())
        .or_insert_with(|| AptosCoinBalanceUpdate {
            aptos_address: owner_address.clone(),
            changed_balances: HashMap::new(),
            sequence_number: tx_version as u64,
            timestamp_ms: chrono::Utc::now().naive_utc().and_utc().timestamp_millis() as u64,
        })
        .changed_balances
        .entry(coin_type.clone())
        .or_insert(vec![])
        .push(AptosCoinUpdate {
            coin_type: coin_type.clone(),
            event_id: gas_event.event_index.to_string(),
            current_total_balance: gas_amount,
            standard: AptosCoinStandard::Coin,
            status: AptosCoinObjectUpdateStatus::Mutated(CoinMutated {
                change: -gas_amount,
            }),
        });

    (coin_type, gas_amount, owner_address)
}

// Helper function to process coin changes
fn process_coin_changes(
    asset_activities: &Vec<(FungibleAssetActivity, CoinAction)>,
    asset_balances: &AHashMap<String, AHashMap<String, FungibleAssetBalance>>,
    ws_updates: &mut AHashMap<String, AptosCoinBalanceUpdate>,
    tx_version: i64,
) {
    for (asset_activity, action) in asset_activities {
        let (coin_type, owner_address) = match (
            asset_activity.asset_type.clone(),
            asset_activity.owner_address.clone(),
        ) {
            (Some(asset_type), Some(owner_address)) => (asset_type, owner_address),
            _ => {
                tracing::warn!(
                    transaction_version = tx_version,
                    "Missing coin type or owner address"
                );
                PROCESSOR_UNKNOWN_TYPE_COUNT
                    .with_label_values(&["NightlyProcessor"])
                    .inc();
                continue;
            },
        };

        // Get coin balance for the owner and coin type
        let coin_balance = match asset_balances
            .get(&owner_address)
            .and_then(|balances| balances.get(&coin_type))
        {
            Some(coin_balance) => coin_balance,
            None => {
                tracing::warn!(
                    transaction_version = tx_version,
                    "Coin balance doesn't exist for the owner {owner_address} and coin type {coin_type}"
                );
                PROCESSOR_UNKNOWN_TYPE_COUNT
                    .with_label_values(&["NightlyProcessor"])
                    .inc();
                continue;
            },
        };

        process_single_coin_activity(
            asset_activity,
            action,
            coin_balance,
            &coin_type,
            &owner_address,
            ws_updates,
            tx_version,
        );
    }
}

// Helper function to process a single coin activity
fn process_single_coin_activity(
    asset_activity: &FungibleAssetActivity,
    action: &CoinAction,
    coin_balance: &FungibleAssetBalance,
    coin_type: &str,
    owner_address: &str,
    ws_updates: &mut AHashMap<String, AptosCoinBalanceUpdate>,
    tx_version: i64,
) {
    let activity_amount = match asset_activity
        .amount
        .clone()
        .and_then(|amount| amount.to_i128())
    {
        Some(amount) => amount,
        None => {
            tracing::warn!(
                transaction_version = tx_version,
                "Invalid or missing activity amount"
            );
            PROCESSOR_UNKNOWN_TYPE_COUNT
                .with_label_values(&["NightlyProcessor"])
                .inc();
            return;
        },
    };

    let coin_balance_amount = match coin_balance.amount.to_i128() {
        Some(amount) => amount,
        None => {
            tracing::warn!(
                transaction_version = tx_version,
                "Invalid coin balance amount"
            );
            PROCESSOR_UNKNOWN_TYPE_COUNT
                .with_label_values(&["NightlyProcessor"])
                .inc();
            return;
        },
    };

    let object_status = determine_coin_status(action, activity_amount, coin_balance_amount);

    ws_updates
        .entry(owner_address.to_string())
        .or_insert_with(|| AptosCoinBalanceUpdate {
            aptos_address: owner_address.to_string(),
            changed_balances: HashMap::new(),
            sequence_number: tx_version as u64,
            timestamp_ms: chrono::Utc::now().naive_utc().and_utc().timestamp_millis() as u64,
        })
        .changed_balances
        .entry(coin_type.to_string())
        .or_insert(vec![])
        .push(AptosCoinUpdate {
            coin_type: coin_type.to_string(),
            event_id: asset_activity.event_index.to_string(),
            current_total_balance: coin_balance_amount,
            standard: match asset_activity.token_standard {
                TokenStandard::V1 => AptosCoinStandard::Coin,
                TokenStandard::V2 => AptosCoinStandard::FungibleAsset,
            },
            status: object_status,
        });
}

// Helper function to determine coin status
fn determine_coin_status(
    action: &CoinAction,
    activity_amount: i128,
    coin_balance_amount: i128,
) -> AptosCoinObjectUpdateStatus {
    if action == &CoinAction::Gas {
        return AptosCoinObjectUpdateStatus::Mutated(CoinMutated {
            change: -activity_amount,
        });
    }

    match coin_balance_amount.cmp(&activity_amount) {
        Ordering::Less => AptosCoinObjectUpdateStatus::Mutated(CoinMutated {
            change: -activity_amount,
        }),
        Ordering::Greater => match action {
            CoinAction::Deposit => AptosCoinObjectUpdateStatus::Mutated(CoinMutated {
                change: activity_amount,
            }),
            CoinAction::Withdraw => AptosCoinObjectUpdateStatus::Mutated(CoinMutated {
                change: -activity_amount,
            }),
            _ => AptosCoinObjectUpdateStatus::Mutated(CoinMutated {
                change: -activity_amount,
            }),
        },
        Ordering::Equal => {
            if coin_balance_amount.is_zero() {
                AptosCoinObjectUpdateStatus::Deleted(CoinDeleted {
                    amount: activity_amount.abs(),
                })
            } else if coin_balance_amount == activity_amount {
                AptosCoinObjectUpdateStatus::Created(CoinCreated {
                    amount: activity_amount,
                })
            } else {
                match action {
                    CoinAction::Freeze => AptosCoinObjectUpdateStatus::Frozen,
                    CoinAction::UnFreeze => AptosCoinObjectUpdateStatus::Unfrozen,
                    _ => AptosCoinObjectUpdateStatus::Mutated(CoinMutated {
                        change: -activity_amount,
                    }),
                }
            }
        },
    }
}

// Helper function to process token changes
fn process_token_changes(
    token_changes: &TransactionTokenChanges,
    ws_updates: &mut AHashMap<String, AptosAccountTokensUpdate>,
    tx_version: i64,
) {
    let mut pending_events: HashMap<String, TokenEventData> = HashMap::new();
    let mut claim_offer_map: HashMap<String, AptosObjectUpdateStatus> = HashMap::new();

    for (token_activity, action) in &token_changes.token_activities {
        let token_data_id = token_activity.token_data_id.clone();
        let event_id = token_activity.event_index.to_string();

        match action {
            TokenAction::Mint => {
                process_token_mint(
                    token_activity,
                    &token_data_id,
                    &event_id,
                    ws_updates,
                    tx_version,
                );
            },
            TokenAction::Burn => {
                process_token_burn(
                    token_activity,
                    &token_data_id,
                    &event_id,
                    ws_updates,
                    tx_version,
                );
            },
            TokenAction::Transfer => {
                process_token_transfer(
                    token_activity,
                    &token_data_id,
                    &event_id,
                    ws_updates,
                    tx_version,
                );
            },
            TokenAction::Mutate => {
                process_token_mutate(
                    token_activity,
                    &token_data_id,
                    &event_id,
                    ws_updates,
                    tx_version,
                );
            },
            TokenAction::Offer => {
                process_token_offer(
                    token_activity,
                    &token_data_id,
                    &event_id,
                    ws_updates,
                    &mut claim_offer_map,
                    tx_version,
                );
            },
            TokenAction::CancelClaim => {
                process_token_cancel_claim(
                    token_activity,
                    &token_data_id,
                    &event_id,
                    ws_updates,
                    tx_version,
                );
            },
            TokenAction::Claim => {
                process_token_claim(
                    token_activity,
                    &token_data_id,
                    &event_id,
                    ws_updates,
                    &mut claim_offer_map,
                    tx_version,
                );
            },
            TokenAction::Deposit | TokenAction::Withdraw => {
                process_token_deposit_withdraw(
                    token_activity,
                    action,
                    &token_data_id,
                    &mut pending_events,
                );
            },
        }
    }
}

// Helper functions for token processing
fn process_token_mint(
    token_activity: &TokenActivityV2,
    token_data_id: &str,
    event_id: &str,
    ws_updates: &mut AHashMap<String, AptosAccountTokensUpdate>,
    tx_version: i64,
) {
    if let Some(address) = &token_activity.from_address {
        add_token_update(
            address,
            token_data_id,
            event_id,
            AptosObjectUpdateStatus::Created,
            token_activity.token_standard.clone(),
            ws_updates,
            tx_version,
        );
    }
}

fn process_token_burn(
    token_activity: &TokenActivityV2,
    token_data_id: &str,
    event_id: &str,
    ws_updates: &mut AHashMap<String, AptosAccountTokensUpdate>,
    tx_version: i64,
) {
    let address = &token_activity.event_account_address;
    add_token_update(
        address,
        token_data_id,
        event_id,
        AptosObjectUpdateStatus::Deleted,
        token_activity.token_standard.clone(),
        ws_updates,
        tx_version,
    );
}

fn process_token_transfer(
    token_activity: &TokenActivityV2,
    token_data_id: &str,
    event_id: &str,
    ws_updates: &mut AHashMap<String, AptosAccountTokensUpdate>,
    tx_version: i64,
) {
    if let (Some(from_address), Some(to_address)) =
        (&token_activity.from_address, &token_activity.to_address)
    {
        // Add sent status for sender
        add_token_update(
            from_address,
            token_data_id,
            event_id,
            AptosObjectUpdateStatus::Sent(Sent {
                sender_address: from_address.clone(),
                receiver_address: to_address.clone(),
            }),
            token_activity.token_standard.clone(),
            ws_updates,
            tx_version,
        );

        // Add received status for receiver
        add_token_update(
            to_address,
            token_data_id,
            event_id,
            AptosObjectUpdateStatus::Received(Received {
                sender_address: from_address.clone(),
                receiver_address: to_address.clone(),
            }),
            token_activity.token_standard.clone(),
            ws_updates,
            tx_version,
        );
    }
}

fn process_token_mutate(
    token_activity: &TokenActivityV2,
    token_data_id: &str,
    event_id: &str,
    ws_updates: &mut AHashMap<String, AptosAccountTokensUpdate>,
    tx_version: i64,
) {
    if let Some(address) = &token_activity.from_address {
        add_token_update(
            address,
            token_data_id,
            event_id,
            AptosObjectUpdateStatus::Mutated,
            token_activity.token_standard.clone(),
            ws_updates,
            tx_version,
        );
    }
}

fn process_token_offer(
    token_activity: &TokenActivityV2,
    token_data_id: &str,
    event_id: &str,
    ws_updates: &mut AHashMap<String, AptosAccountTokensUpdate>,
    claim_offer_map: &mut HashMap<String, AptosObjectUpdateStatus>,
    tx_version: i64,
) {
    if let (Some(owner_address), Some(receiver_address)) =
        (&token_activity.from_address, &token_activity.to_address)
    {
        let offer_status = AptosObjectUpdateStatus::Offer(Offer {
            sender_address: owner_address.clone(),
            receiver_address: receiver_address.clone(),
        });

        claim_offer_map.insert(token_data_id.to_string(), offer_status.clone());

        // Add offer status for sender
        add_token_update(
            owner_address,
            token_data_id,
            event_id,
            offer_status,
            token_activity.token_standard.clone(),
            ws_updates,
            tx_version,
        );

        // Add pending claim status for receiver
        add_token_update(
            receiver_address,
            token_data_id,
            event_id,
            AptosObjectUpdateStatus::PendingClaim(PendingClaim {
                sender_address: owner_address.clone(),
                receiver_address: receiver_address.clone(),
            }),
            token_activity.token_standard.clone(),
            ws_updates,
            tx_version,
        );
    }
}

fn process_token_cancel_claim(
    token_activity: &TokenActivityV2,
    token_data_id: &str,
    event_id: &str,
    ws_updates: &mut AHashMap<String, AptosAccountTokensUpdate>,
    tx_version: i64,
) {
    if let Some(address) = &token_activity.from_address {
        add_token_update(
            address,
            token_data_id,
            event_id,
            AptosObjectUpdateStatus::CancelClaim,
            token_activity.token_standard.clone(),
            ws_updates,
            tx_version,
        );
    }
}

fn process_token_claim(
    token_activity: &TokenActivityV2,
    token_data_id: &str,
    event_id: &str,
    ws_updates: &mut AHashMap<String, AptosAccountTokensUpdate>,
    claim_offer_map: &mut HashMap<String, AptosObjectUpdateStatus>,
    tx_version: i64,
) {
    if let Some(receiver_address) = &token_activity.to_address {
        claim_offer_map.insert(token_data_id.to_string(), AptosObjectUpdateStatus::Claim);

        add_token_update(
            receiver_address,
            token_data_id,
            event_id,
            AptosObjectUpdateStatus::Claim,
            token_activity.token_standard.clone(),
            ws_updates,
            tx_version,
        );
    }
}

fn process_token_deposit_withdraw(
    token_activity: &TokenActivityV2,
    action: &TokenAction,
    token_data_id: &str,
    pending_events: &mut HashMap<String, TokenEventData>,
) {
    let event_data = pending_events
        .entry(token_data_id.to_string())
        .or_insert(TokenEventData {
            from_address: token_activity.from_address.clone(),
            to_address: token_activity.to_address.clone(),
            token_data_id: token_data_id.to_string(),
            event_index: token_activity.event_index,
        });

    match action {
        TokenAction::Deposit => {
            event_data.to_address = token_activity.to_address.clone();
        },
        TokenAction::Withdraw => {
            event_data.from_address = token_activity.from_address.clone();
        },
        _ => {},
    }
}

fn add_token_update(
    address: &str,
    token_data_id: &str,
    event_id: &str,
    status: AptosObjectUpdateStatus,
    token_standard: TokenStandard,
    ws_updates: &mut AHashMap<String, AptosAccountTokensUpdate>,
    tx_version: i64,
) {
    ws_updates
        .entry(address.to_string())
        .or_insert_with(|| AptosAccountTokensUpdate {
            aptos_address: address.to_string(),
            tokens_changes: HashMap::new(),
            sequence_number: tx_version as u64,
            timestamp_ms: chrono::Utc::now().naive_utc().and_utc().timestamp_millis() as u64,
        })
        .tokens_changes
        .entry(token_data_id.to_string())
        .or_insert_with(Vec::new)
        .push(AptosTokenChangeUpdate {
            token_id: token_data_id.to_string(),
            event_id: event_id.to_string(),
            status,
            standard: match token_standard {
                TokenStandard::V1 => AptosTokenStandard::Token,
                TokenStandard::V2 => AptosTokenStandard::DigitalAsset,
            },
        });
}

// Helper function to generate notifications
fn generate_notifications(
    coin_updates: &AHashMap<String, AptosCoinBalanceUpdate>,
    token_updates: &AHashMap<String, AptosAccountTokensUpdate>,
    notifications: &mut Vec<AptosIndexerNotification>,
    gas_type: &str,
    gas_amount: i128,
    gas_payee: &String,
    nudge_events: &Vec<IndexerNudge>,
) {
    // Generate coin notifications
    for (account_address, update) in coin_updates {
        let mut aggregated_changes: HashMap<String, i128> = HashMap::new();

        // Aggregate changes by coin_type
        for (coin_type, balance_updates) in &update.changed_balances {
            for coin_update in balance_updates {
                let entry = aggregated_changes.entry(coin_type.clone()).or_insert(0);

                // Combine changes for the same coin_type
                match &coin_update.status {
                    AptosCoinObjectUpdateStatus::Mutated(change) => {
                        *entry += change.change;
                    },
                    AptosCoinObjectUpdateStatus::Created(_) => {
                        *entry += coin_update.current_total_balance;
                    },
                    AptosCoinObjectUpdateStatus::Deleted(deleted) => {
                        *entry -= deleted.amount;
                    },
                    AptosCoinObjectUpdateStatus::Frozen => {
                        notifications.push(AptosIndexerNotification::CoinFrozen(CoinFrozen {
                            aptos_address: account_address.clone(),
                            coin_type: coin_type.clone(),
                            frozen: true,
                        }));
                    },
                    AptosCoinObjectUpdateStatus::Unfrozen => {
                        notifications.push(AptosIndexerNotification::CoinFrozen(CoinFrozen {
                            aptos_address: account_address.clone(),
                            coin_type: coin_type.clone(),
                            frozen: false,
                        }));
                    },
                }
            }

            if coin_type == gas_type && account_address == gas_payee {
                let entry = aggregated_changes.entry(coin_type.clone()).or_insert(0);
                // gas amount is negative
                *entry += gas_amount;
            }
        }

        let mut spent = Vec::new();
        let mut received = Vec::new();
        let mut only_negative = true;
        let mut only_positive = true;

        // Process aggregated changes
        for (coin_type, total_change) in aggregated_changes {
            if total_change < 0 {
                // Coin sent (negative change)
                spent.push((coin_type.clone(), -total_change)); // Keep amount positive
                only_positive = false;
            } else if total_change > 0 {
                // Coin received (positive change)
                received.push((coin_type.clone(), total_change));
                only_negative = false;
            }
        }

        // Determine the appropriate notification
        if only_negative && !spent.is_empty() {
            // If only negative changes, it's a CoinSent event
            for (coin_type, amount) in spent {
                notifications.push(AptosIndexerNotification::CoinSent(CoinSent {
                    sender_address: account_address.clone(),
                    coin_type,
                    amount,
                }));
            }
        } else if only_positive && !received.is_empty() {
            // If only positive changes, it's a CoinReceived event
            for (coin_type, amount) in received {
                notifications.push(AptosIndexerNotification::CoinReceived(CoinReceived {
                    receiver_address: account_address.clone(),
                    coin_type,
                    amount,
                }));
            }
        } else if !spent.is_empty() && !received.is_empty() {
            // If there are both negative and positive changes, it's a CoinSwap event
            notifications.push(AptosIndexerNotification::CoinSwap(CoinSwap {
                aptos_address: account_address.clone(),
                spent,
                received,
            }));
        }
    }

    // Generate token notifications
    for (account_address, token_update) in token_updates {
        for (token_id, token_changes) in &token_update.tokens_changes {
            for token_change in token_changes {
                match &token_change.status {
                    AptosObjectUpdateStatus::Created => {
                        notifications.push(AptosIndexerNotification::NftMinted(NftMinted {
                            aptos_address: account_address.clone(),
                            token_data_id: token_id.clone(),
                        }));
                    },
                    AptosObjectUpdateStatus::Offer(offer) => {
                        notifications.push(AptosIndexerNotification::NftOffer(NftOffer {
                            sender: offer.sender_address.clone(),
                            receiver: offer.receiver_address.clone(),
                            token_data_id: token_id.clone(),
                        }));
                    },
                    AptosObjectUpdateStatus::Claim => {
                        notifications.push(AptosIndexerNotification::NftClaim(NftClaim {
                            receiver: account_address.clone(),
                            token_data_id: token_id.clone(),
                        }));
                    },
                    AptosObjectUpdateStatus::CancelClaim => {
                        notifications.push(AptosIndexerNotification::NftCancelClaim(
                            NftCancelClaim {
                                aptos_address: account_address.clone(),
                                token_data_id: token_id.clone(),
                            },
                        ));
                    },
                    AptosObjectUpdateStatus::Deleted => {
                        notifications.push(AptosIndexerNotification::NftBurned(NftBurned {
                            aptos_address: account_address.clone(),
                            token_data_id: token_id.clone(),
                        }));
                    },
                    AptosObjectUpdateStatus::Sent(sent) => {
                        notifications.push(AptosIndexerNotification::NftSent(NftSent {
                            sender_address: sent.sender_address.clone(),
                            token_data_id: token_id.clone(),
                        }));
                    },
                    AptosObjectUpdateStatus::Received(received) => {
                        notifications.push(AptosIndexerNotification::NftReceived(NftReceived {
                            receiver_address: received.receiver_address.clone(),
                            token_data_id: token_id.clone(),
                        }));
                    },
                    _ => {}, // Skip other status types
                }
            }
        }
    }

    // Generate nudge notifications
    for nudge in nudge_events {
        notifications.push(AptosIndexerNotification::Nudge(Nudge {
            sender: nudge.nudge_sender.clone(),
            receiver: nudge.nudge_receiver.clone(),
        }));
    }
}
