// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use super::{DefaultProcessingResult, ProcessorName, ProcessorTrait};
use crate::{
    db::postgres::models::{
        fungible_asset_models::{
            v2_fungible_asset_activities::{EventToCoinType, FungibleAssetActivity},
            v2_fungible_asset_balances::FungibleAssetBalance,
            v2_fungible_asset_utils::{CoinAction, FeeStatement, FungibleAssetMetadata},
        },
        object_models::v2_object_utils::{
            ObjectAggregatedData, ObjectAggregatedDataMapping, ObjectWithMetadata, Untransferable,
        },
        resources::{FromWriteResource, V2FungibleAssetResource, V2TokenResource},
        token_models::{
            token_claims::CurrentTokenPendingClaim,
            tokens::{CurrentTokenPendingClaimPK, TableMetadataForToken},
        },
        token_v2_models::{
            v2_token_activities::{TokenAction, TokenActivityV2},
            v2_token_datas::TokenDataV2,
            v2_token_ownerships::{
                CurrentTokenOwnershipV2, CurrentTokenOwnershipV2PK, NFTOwnershipV2,
                TokenOwnershipV2,
            },
            v2_token_utils::{
                Burn, BurnEvent, MintEvent, TokenStandard, TokenV2Burned, TokenV2Minted,
                TransferEvent,
            },
        },
    },
    gap_detectors::ProcessingResult,
    nats_queue::NatsQueueSender,
    processors::nightly_processors_helpers::process_changes,
    utils::{
        counters::PROCESSOR_UNKNOWN_TYPE_COUNT,
        database::ArcDbPool,
        util::{get_entry_function_from_user_request, standardize_address},
    },
    IndexerGrpcProcessorConfig,
};
use ahash::{AHashMap, AHashSet};
use aptos_protos::transaction::v1::{
    transaction::TxnData, write_set_change::Change, Event, Transaction, TransactionInfo,
    UserTransactionRequest,
};
use async_trait::async_trait;
use bigdecimal::{BigDecimal, ToPrimitive, Zero};
use chrono::NaiveDateTime;
use core::panic;
use odin::structs::{
    notifications::aptos_notifications::{
        AptosIndexerNotification, CoinFrozen, CoinReceived, CoinSent, CoinSwap, NftBurned,
        NftCancelClaim, NftClaim, NftMinted, NftOffer, NftReceived, NftSent,
    },
    ws::{
        aptos_ws::{
            AptosAccountTokensUpdate, AptosCoinBalanceUpdate, AptosCoinObjectUpdateStatus,
            AptosCoinStandard, AptosObjectUpdateStatus, AptosTokenChangeUpdate, AptosTokenStandard,
            AptosWsApiMsg, CoinUpdate, Offer, PendingClaim,
        },
        ws_message::{CoinCreated, CoinDeleted, CoinMutated, Received, Sent},
    },
};
use rayon::iter::{IntoParallelRefIterator, ParallelIterator};
use serde::{Deserialize, Serialize};
use serde_json::to_string_pretty;
use std::{cmp::Ordering, collections::HashMap, fmt::Debug, fs::OpenOptions, sync::Arc};
use tracing::warn;

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct NightlyProcessorConfig {
    // Leaving this here for now, but we should remove it
    #[serde(default = "IndexerGrpcProcessorConfig::default_query_retries")]
    pub query_retries: u32,
    #[serde(default = "IndexerGrpcProcessorConfig::default_query_retry_delay_ms")]
    pub query_retry_delay_ms: u64,
}

#[derive(Debug)]
pub struct NightlyProcessor {
    connection_pool: ArcDbPool,
    config: NightlyProcessorConfig,
    queue_sender: Arc<NatsQueueSender>,
}

impl NightlyProcessor {
    pub fn new(
        connection_pool: ArcDbPool,
        config: NightlyProcessorConfig,
        queue_sender: Arc<NatsQueueSender>,
    ) -> Self {
        Self {
            connection_pool,
            config,
            queue_sender,
        }
    }
}

#[async_trait]
impl ProcessorTrait for NightlyProcessor {
    fn name(&self) -> &'static str {
        ProcessorName::TokenV2Processor.into()
    }

    async fn process_transactions(
        &self,
        transactions: Vec<Transaction>,
        start_version: u64,
        end_version: u64,
        _: Option<u64>,
    ) -> anyhow::Result<ProcessingResult> {
        let processing_start = std::time::Instant::now();
        let last_transaction_timestamp = transactions.last().unwrap().timestamp.clone();

        // First get all token related table metadata from the batch of transactions. This is in case
        // an earlier transaction has metadata (in resources) that's missing from a later transaction.
        let table_handle_to_owner =
            TableMetadataForToken::get_table_handle_to_owner_from_transactions(&transactions);

        let parsed_transactions = parse_v2_token(&transactions, table_handle_to_owner);

        let (ws_updates, notifications) = process_changes(parsed_transactions);

        self.queue_sender
            .ws_sender
            .send((start_version, end_version, ws_updates))
            .await
            .unwrap();

        self.queue_sender
            .notifications_sender
            .send((start_version, end_version, notifications))
            .await
            .unwrap();

        let processing_duration_in_secs = processing_start.elapsed().as_secs_f64();

        println!(
            "Processed transactions from {} to {}, total {} in {} seconds",
            start_version,
            end_version,
            end_version - start_version,
            processing_duration_in_secs
        );

        // Process the transactions and send them to the nats queue
        Ok(ProcessingResult::DefaultProcessingResult(
            DefaultProcessingResult {
                start_version,
                end_version,
                processing_duration_in_secs,
                db_insertion_duration_in_secs: 0.0,
                last_transaction_timestamp,
            },
        ))
    }

    fn connection_pool(&self) -> &ArcDbPool {
        &self.connection_pool
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TransactionChanges {
    pub txn_version: i64,
    pub gas_used: i128,
    pub coin_changes: TransactionCoinChanges,
    pub token_changes: TransactionTokenChanges,
}
use std::io::Write;

fn parse_v2_token(
    transactions: &[Transaction],
    table_handle: AHashMap<String, TableMetadataForToken>,
) -> Vec<TransactionChanges> {
    transactions
        .par_iter()
        .filter_map(|txn| {
            let txn_version = txn.version as i64;

            let block_height = txn.block_height as i64;
            let txn_data = match txn.txn_data.as_ref() {
                Some(data) => data,
                None => {
                    tracing::warn!(
                        transaction_version = txn_version,
                        "Transaction data doesn't exist"
                    );
                    PROCESSOR_UNKNOWN_TYPE_COUNT
                        .with_label_values(&["NightlyProcessor"])
                        .inc();
                    return None;
                },
            };

            let transaction_info = txn.info.as_ref().expect("Transaction info doesn't exist!");
            let txn_timestamp = txn
                .timestamp
                .as_ref()
                .expect("Transaction timestamp doesn't exist!")
                .seconds;
            #[allow(deprecated)]
            let txn_timestamp = NaiveDateTime::from_timestamp_opt(txn_timestamp, 0)
                .expect("Txn Timestamp is invalid!");

            let default = vec![];
            let (events, user_request, entry_function_id_str) = match txn_data {
                TxnData::User(tx_inner) => {
                    let user_request = tx_inner
                        .request
                        .as_ref()
                        .expect("Sends is not present in user txn");
                    let entry_function_id_str = get_entry_function_from_user_request(user_request);
                    (&tx_inner.events, Some(user_request), entry_function_id_str)
                },
                _ => (&default, None, None),
            };

            let transaction_coin_changes = process_transaction_coins_changes(
                txn_version,
                block_height,
                transaction_info,
                txn_timestamp,
                events,
                &user_request.cloned(),
                &entry_function_id_str,
                BigDecimal::from(transaction_info.gas_used),
            );

            let transaction_token_changes = process_transaction_tokens_changes(
                txn_version,
                transaction_info,
                txn_timestamp,
                events,
                &entry_function_id_str,
                &table_handle,
            );

            // 1.2042955419
            // 2. 2043132209
            // 3. nudge from seeds 1 to seeds 2
            if txn_version == 2054770372 {
                // Convert transaction to pretty JSON string

                fn write_to_file<T: serde::Serialize>(data: &T, filename: &str) {
                    let json_string = to_string_pretty(&data)
                        .expect(&format!("Failed to serialize {} to JSON", filename));

                    let mut file = OpenOptions::new()
                        .create(true)
                        .write(true) // Using write instead of append to overwrite
                        .truncate(true)
                        .open(format!("dump_{}.json", filename))
                        .expect(&format!("Failed to open {}", filename));

                    file.write_all(json_string.as_bytes())
                        .expect(&format!("Failed to write {}", filename));
                }

                // Save each piece of data to its own file
                // write_to_file(&txn_data, "txn_data");
                write_to_file(&transaction_info, "transaction_info");
                write_to_file(&txn_timestamp, "txn_timestamp");
                write_to_file(&events, "events");
                write_to_file(&user_request, "user_request");
                write_to_file(&transaction_token_changes, "transaction_token_changes");

                println!("All transaction data has been dumped to separate files");
                panic!("Transaction dumped to files");
            }

            Some(TransactionChanges {
                txn_version,
                gas_used: transaction_info.gas_used as i128,
                coin_changes: transaction_coin_changes,
                token_changes: transaction_token_changes,
            })
        })
        .collect()
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TransactionCoinChanges {
    pub txn_version: i64,
    pub gas_event: Option<FungibleAssetActivity>,
    pub asset_balances: AHashMap<String, AHashMap<String, FungibleAssetBalance>>, // owner_address -> asset_type -> balance
    pub asset_activities: Vec<(FungibleAssetActivity, CoinAction)>,
}

fn process_transaction_coins_changes(
    txn_version: i64,
    block_height: i64,
    transaction_info: &TransactionInfo,
    txn_timestamp: NaiveDateTime,
    events: &Vec<Event>,
    user_request: &Option<UserTransactionRequest>,
    entry_function_id_str: &Option<String>,
    txn_gas_used: BigDecimal,
) -> TransactionCoinChanges {
    let mut fungible_asset_activities = vec![];
    let mut fungible_asset_balances = AHashMap::new();

    // Get Metadata for fungible assets by object
    let mut fungible_asset_object_helper: ObjectAggregatedDataMapping = AHashMap::new();

    // This is because v1 events (deposit/withdraw) don't have coin type so the only way is to match
    // the event to the resource using the event guid
    let mut event_to_v1_coin_type: EventToCoinType = AHashMap::new();

    // Loop 1: to get all object addresses
    // Need to do a first pass to get all the object addresses and insert them into the helper
    for wsc in transaction_info.changes.iter() {
        if let Change::WriteResource(wr) = wsc.change.as_ref().unwrap() {
            if let Some(object) = ObjectWithMetadata::from_write_resource(wr).unwrap() {
                fungible_asset_object_helper.insert(
                    standardize_address(&wr.address.to_string()),
                    ObjectAggregatedData {
                        object,
                        ..ObjectAggregatedData::default()
                    },
                );
            }
        }
    }
    // Loop 2: Get the metadata relevant to parse v1 coin and v2 fungible asset.
    // As an optimization, we also handle v1 balances in the process
    for (index, wsc) in transaction_info.changes.iter().enumerate() {
        if let Change::WriteResource(write_resource) = wsc.change.as_ref().unwrap() {
            if let Some((balance, _current_balance, event_to_coin)) =
                FungibleAssetBalance::get_v1_from_write_resource(
                    write_resource,
                    index as i64,
                    txn_version,
                    txn_timestamp,
                )
                .unwrap()
            {
                fungible_asset_balances
                    .entry(balance.owner_address.clone())
                    .or_insert_with(AHashMap::new)
                    .insert(balance.asset_type.clone(), balance);

                event_to_v1_coin_type.extend(event_to_coin);
            }
            // Fill the v2 fungible_asset_object_helper. This is used to track which objects exist at each object address.
            // The data will be used to reconstruct the full data in Loop 4.
            let address = standardize_address(&write_resource.address.to_string());
            if let Some(aggregated_data) = fungible_asset_object_helper.get_mut(&address) {
                if let Some(v2_fungible_asset_resource) =
                    V2FungibleAssetResource::from_write_resource(write_resource).unwrap()
                {
                    match v2_fungible_asset_resource {
                        V2FungibleAssetResource::FungibleAssetMetadata(fungible_asset_metadata) => {
                            aggregated_data.fungible_asset_metadata = Some(fungible_asset_metadata);
                        },
                        V2FungibleAssetResource::FungibleAssetStore(fungible_asset_store) => {
                            aggregated_data.fungible_asset_store = Some(fungible_asset_store);
                        },
                        V2FungibleAssetResource::FungibleAssetSupply(fungible_asset_supply) => {
                            aggregated_data.fungible_asset_supply = Some(fungible_asset_supply);
                        },
                        V2FungibleAssetResource::ConcurrentFungibleAssetSupply(
                            concurrent_fungible_asset_supply,
                        ) => {
                            aggregated_data.concurrent_fungible_asset_supply =
                                Some(concurrent_fungible_asset_supply);
                        },
                        V2FungibleAssetResource::ConcurrentFungibleAssetBalance(
                            concurrent_fungible_asset_balance,
                        ) => {
                            aggregated_data.concurrent_fungible_asset_balance =
                                Some(concurrent_fungible_asset_balance);
                        },
                    }
                }
            }
        } else if let Change::DeleteResource(delete_resource) = wsc.change.as_ref().unwrap() {
            if let Some((balance, _, event_to_coin)) =
                FungibleAssetBalance::get_v1_from_delete_resource(
                    delete_resource,
                    index as i64,
                    txn_version,
                    txn_timestamp,
                )
                .unwrap()
            {
                fungible_asset_balances
                    .entry(balance.owner_address.clone())
                    .or_insert_with(AHashMap::new)
                    .insert(balance.asset_type.clone(), balance);
                event_to_v1_coin_type.extend(event_to_coin);
            }
        }
    }

    let mut final_gas_event: Option<FungibleAssetActivity> = None;
    let mut final_gas_amount = txn_gas_used;

    // The artificial gas event, only need for v1
    if let Some(req) = user_request {
        // Update has used gas
        final_gas_amount = final_gas_amount * req.gas_unit_price;

        let fee_statement = events.iter().find_map(|event| {
            let event_type = event.type_str.as_str();
            FeeStatement::from_event(event_type, &event.data, txn_version)
        });
        let gas_event = FungibleAssetActivity::get_gas_event(
            transaction_info,
            req,
            &entry_function_id_str,
            txn_version,
            txn_timestamp,
            block_height,
            fee_statement,
        );

        final_gas_event = Some(gas_event)
    }

    // Loop 3 to handle events and collect additional metadata from events for v2
    for (index, event) in events.iter().enumerate() {
        if let Some((v1_activity, action)) = FungibleAssetActivity::get_v1_from_event(
            event,
            txn_version,
            block_height,
            txn_timestamp,
            &entry_function_id_str,
            &event_to_v1_coin_type,
            index as i64,
        )
        .unwrap_or_else(|e| {
            tracing::error!(
                    transaction_version = txn_version,
                    index = index,
                    error = ?e,
                    "[Parser] error parsing fungible asset activity v1");
            panic!("[Parser] error parsing fungible asset activity v1");
        }) {
            fungible_asset_activities.push((v1_activity, action));
        }
        if let Some((v2_activity, action)) = FungibleAssetActivity::get_v2_from_event(
            event,
            txn_version,
            block_height,
            txn_timestamp,
            index as i64,
            &entry_function_id_str,
            &fungible_asset_object_helper,
        )
        .unwrap_or_else(|e| {
            tracing::error!(
                    transaction_version = txn_version,
                    index = index,
                    error = ?e,
                    "[Parser] error parsing fungible asset activity v2");
            panic!("[Parser] error parsing fungible asset activity v2");
        }) {
            if action == CoinAction::Withdraw
                && v2_activity.amount == Some(final_gas_amount.clone())
            {
                final_gas_event = Some(v2_activity.clone());
            } else {
                fungible_asset_activities.push((v2_activity, action));
            }
        }
    }

    // Loop 4 to handle write set changes for metadata, balance, and v1 supply
    for (index, wsc) in transaction_info.changes.iter().enumerate() {
        match wsc.change.as_ref().unwrap() {
            Change::WriteResource(write_resource) => {
                if let Some((balance, _curr_balance)) =
                    FungibleAssetBalance::get_v2_from_write_resource(
                        write_resource,
                        index as i64,
                        txn_version,
                        txn_timestamp,
                        &fungible_asset_object_helper,
                    )
                    .unwrap_or_else(|e| {
                        tracing::error!(
                            transaction_version = txn_version,
                            index = index,
                                error = ?e,
                            "[Parser] error parsing fungible balance v2");
                        panic!("[Parser] error parsing fungible balance v2");
                    })
                {
                    fungible_asset_balances
                        .entry(balance.owner_address.clone())
                        .or_insert_with(AHashMap::new)
                        .insert(balance.asset_type.clone(), balance);
                }
            },
            _ => {},
        }
    }

    TransactionCoinChanges {
        txn_version,
        gas_event: final_gas_event,
        asset_balances: fungible_asset_balances,
        asset_activities: fungible_asset_activities,
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TransactionTokenChanges {
    pub txn_version: i64,
    pub token_activities: Vec<(TokenActivityV2, TokenAction)>,
}

fn process_transaction_tokens_changes(
    txn_version: i64,
    transaction_info: &TransactionInfo,
    txn_timestamp: NaiveDateTime,
    events: &Vec<Event>,
    entry_function_id_str: &Option<String>,
    table_handle_to_owner: &AHashMap<String, TableMetadataForToken>,
) -> TransactionTokenChanges {
    // Token V2 and V1 combined
    let mut token_activities_v2 = vec![];
    let mut token_v2_metadata_helper: ObjectAggregatedDataMapping = AHashMap::new();

    let mut current_token_ownerships_v2: AHashMap<
        CurrentTokenOwnershipV2PK,
        CurrentTokenOwnershipV2,
    > = AHashMap::new();
    let mut current_deleted_token_ownerships_v2 = AHashMap::new();
    // Tracks prior ownership in case a token gets burned
    let mut prior_nft_ownership: AHashMap<String, NFTOwnershipV2> = AHashMap::new();

    // migrating this from v1 token model as we don't have any replacement table for this
    let mut all_current_token_claims: AHashMap<
        CurrentTokenPendingClaimPK,
        CurrentTokenPendingClaim,
    > = AHashMap::new();

    // Get burn events for token v2 by object
    let mut tokens_burned: TokenV2Burned = AHashMap::new();

    // Get mint events for token v2 by object
    let mut tokens_minted: TokenV2Minted = AHashSet::new();

    // Loop 1: Need to do a first pass to get all the object addresses and insert them into the helper
    for wsc in transaction_info.changes.iter() {
        if let Change::WriteResource(wr) = wsc.change.as_ref().unwrap() {
            if let Some(object) = ObjectWithMetadata::from_write_resource(wr).unwrap() {
                token_v2_metadata_helper.insert(
                    standardize_address(&wr.address.to_string()),
                    ObjectAggregatedData {
                        object,
                        ..ObjectAggregatedData::default()
                    },
                );
            }
        }
    }

    // Loop 2: Get the metdata relevant to parse v1 and v2 tokens
    // Need to do a second pass to get all the structs related to the object
    for wsc in transaction_info.changes.iter() {
        if let Change::WriteResource(wr) = wsc.change.as_ref().unwrap() {
            let address = standardize_address(&wr.address.to_string());
            if let Some(aggregated_data) = token_v2_metadata_helper.get_mut(&address) {
                if let Some(v2_token_resource) = V2TokenResource::from_write_resource(wr).unwrap() {
                    match v2_token_resource {
                        V2TokenResource::FixedSupply(fixed_supply) => {
                            aggregated_data.fixed_supply = Some(fixed_supply);
                        },
                        V2TokenResource::UnlimitedSupply(unlimited_supply) => {
                            aggregated_data.unlimited_supply = Some(unlimited_supply);
                        },
                        V2TokenResource::AptosCollection(aptos_collection) => {
                            aggregated_data.aptos_collection = Some(aptos_collection);
                        },
                        V2TokenResource::PropertyMapModel(property_map) => {
                            aggregated_data.property_map = Some(property_map);
                        },
                        V2TokenResource::ConcurrentSupply(concurrent_supply) => {
                            aggregated_data.concurrent_supply = Some(concurrent_supply);
                        },
                        V2TokenResource::TokenV2(token) => {
                            aggregated_data.token = Some(token);
                        },
                        V2TokenResource::TokenIdentifiers(token_identifier) => {
                            aggregated_data.token_identifier = Some(token_identifier);
                        },
                        V2TokenResource::Untransferable(untransferable) => {
                            aggregated_data.untransferable = Some(untransferable);
                        },
                        _ => {},
                    }
                }
                if let Some(fungible_asset_metadata) =
                    FungibleAssetMetadata::from_write_resource(wr).unwrap()
                {
                    aggregated_data.fungible_asset_metadata = Some(fungible_asset_metadata);
                }
            }
        }
    }

    // Loop 3: Pass through events to get the burn events and token activities v2
    // This needs to be here because we need the metadata parsed in loop 2 for token activities
    // and burn / transfer events need to come before the next loop
    for (index, event) in events.iter().enumerate() {
        if let Some(burn_event) = Burn::from_event(event, txn_version).unwrap() {
            tokens_burned.insert(burn_event.get_token_address(), burn_event);
        }
        if let Some(old_burn_event) = BurnEvent::from_event(event, txn_version).unwrap() {
            let burn_event = Burn::new(
                standardize_address(event.key.as_ref().unwrap().account_address.as_str()),
                old_burn_event.get_token_address(),
                "".to_string(),
            );
            tokens_burned.insert(burn_event.get_token_address(), burn_event);
        }
        if let Some(mint_event) = MintEvent::from_event(event, txn_version).unwrap() {
            tokens_minted.insert(mint_event.get_token_address());
        }
        if let Some(transfer_events) = TransferEvent::from_event(event, txn_version).unwrap() {
            if let Some(aggregated_data) =
                token_v2_metadata_helper.get_mut(&transfer_events.get_object_address())
            {
                // we don't want index to be 0 otherwise we might have collision with write set change index
                // note that these will be multiplied by -1 so that it doesn't conflict with wsc index
                let index = if index == 0 { events.len() } else { index };
                aggregated_data
                    .transfer_events
                    .push((index as i64, transfer_events));
            }
        }
        // handling all the token v1 events
        if let Some((event, action)) = TokenActivityV2::get_v1_from_parsed_event(
            event,
            txn_version,
            txn_timestamp,
            index as i64,
            &entry_function_id_str,
        )
        .unwrap()
        {
            token_activities_v2.push((event, action));
        }
        // handling all the token v2 events
        if let Some((event, action)) = TokenActivityV2::get_nft_v2_from_parsed_event(
            event,
            txn_version,
            txn_timestamp,
            index as i64,
            &entry_function_id_str,
            &token_v2_metadata_helper,
        )
        .unwrap()
        {
            token_activities_v2.push((event, action));
        }
    }

    // Loop 4: Pass through the changes for collection, token data, token ownership, and token royalties
    for (index, wsc) in transaction_info.changes.iter().enumerate() {
        let wsc_index = index as i64;
        match wsc.change.as_ref().unwrap() {
            Change::WriteTableItem(table_item) => {
                if let Some((_token_ownership, current_token_ownership)) =
                    TokenOwnershipV2::get_v1_from_write_table_item(
                        table_item,
                        txn_version,
                        wsc_index,
                        txn_timestamp,
                        table_handle_to_owner,
                    )
                    .unwrap()
                {
                    if let Some(cto) = current_token_ownership {
                        prior_nft_ownership.insert(
                            cto.token_data_id.clone(),
                            NFTOwnershipV2 {
                                token_data_id: cto.token_data_id.clone(),
                                owner_address: cto.owner_address.clone(),
                                is_soulbound: cto.is_soulbound_v2,
                            },
                        );
                        current_token_ownerships_v2.insert(
                            (
                                cto.token_data_id.clone(),
                                cto.property_version_v1.clone(),
                                cto.owner_address.clone(),
                                cto.storage_id.clone(),
                            ),
                            cto,
                        );
                    }
                }
                if let Some(current_token_token_claim) =
                    CurrentTokenPendingClaim::from_write_table_item(
                        table_item,
                        txn_version,
                        txn_timestamp,
                        table_handle_to_owner,
                    )
                    .unwrap()
                {
                    all_current_token_claims.insert(
                        (
                            current_token_token_claim.token_data_id_hash.clone(),
                            current_token_token_claim.property_version.clone(),
                            current_token_token_claim.from_address.clone(),
                            current_token_token_claim.to_address.clone(),
                        ),
                        current_token_token_claim,
                    );
                }
            },
            Change::DeleteTableItem(table_item) => {
                if let Some((_token_ownership, current_token_ownership)) =
                    TokenOwnershipV2::get_v1_from_delete_table_item(
                        table_item,
                        txn_version,
                        wsc_index,
                        txn_timestamp,
                        table_handle_to_owner,
                    )
                    .unwrap()
                {
                    if let Some(cto) = current_token_ownership {
                        prior_nft_ownership.insert(
                            cto.token_data_id.clone(),
                            NFTOwnershipV2 {
                                token_data_id: cto.token_data_id.clone(),
                                owner_address: cto.owner_address.clone(),
                                is_soulbound: cto.is_soulbound_v2,
                            },
                        );
                        current_deleted_token_ownerships_v2.insert(
                            (
                                cto.token_data_id.clone(),
                                cto.property_version_v1.clone(),
                                cto.owner_address.clone(),
                                cto.storage_id.clone(),
                            ),
                            cto,
                        );
                    }
                }
                if let Some(current_token_token_claim) =
                    CurrentTokenPendingClaim::from_delete_table_item(
                        table_item,
                        txn_version,
                        txn_timestamp,
                        table_handle_to_owner,
                    )
                    .unwrap()
                {
                    all_current_token_claims.insert(
                        (
                            current_token_token_claim.token_data_id_hash.clone(),
                            current_token_token_claim.property_version.clone(),
                            current_token_token_claim.from_address.clone(),
                            current_token_token_claim.to_address.clone(),
                        ),
                        current_token_token_claim,
                    );
                }
            },
            Change::WriteResource(resource) => {
                if let Some((token_data, _current_token_data)) =
                    TokenDataV2::get_v2_from_write_resource(
                        resource,
                        txn_version,
                        wsc_index,
                        txn_timestamp,
                        &token_v2_metadata_helper,
                    )
                    .unwrap()
                {
                    // Add NFT ownership
                    let (ownerships, current_ownerships) =
                        TokenOwnershipV2::get_nft_v2_from_token_data(
                            &token_data,
                            &token_v2_metadata_helper,
                        )
                        .unwrap();
                    if let Some(current_nft_ownership) = ownerships.first() {
                        // Note that the first element in ownerships is the current ownership. We need to cache
                        // it in prior_nft_ownership so that moving forward if we see a burn we'll know
                        // where it came from.
                        prior_nft_ownership.insert(
                            current_nft_ownership.token_data_id.clone(),
                            NFTOwnershipV2 {
                                token_data_id: current_nft_ownership.token_data_id.clone(),
                                owner_address: current_nft_ownership
                                    .owner_address
                                    .as_ref()
                                    .unwrap()
                                    .clone(),
                                is_soulbound: current_nft_ownership.is_soulbound_v2,
                            },
                        );
                    }

                    current_token_ownerships_v2.extend(current_ownerships);
                }
            },
            _ => {},
        }
    }

    return TransactionTokenChanges {
        txn_version,
        token_activities: token_activities_v2,
    };
}

// pub struct NudgeAction {
//     pub txn_version: i64,
//     pub nudge_sender: String,
//     pub nudge_receiver: String,
// }

// fn look_for_nudge_events(events: &Vec<Event>) -> Vec<Event> {
//     let mut nudge_events = vec![];
//     for event in events {
//         if event.type_str == "nudge" {
//             nudge_events.push(event.clone());
//         }
//     }
//     nudge_events
// }
