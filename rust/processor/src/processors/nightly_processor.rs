// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use super::{DefaultProcessingResult, ProcessorName, ProcessorTrait};
use crate::db::common::models::{
    fungible_asset_models::v2_fungible_asset_utils::CoinAction,
    token_v2_models::v2_token_activities::{TokenAction, TokenActivityV2},
};
use crate::{
    db::common::models::{
        fungible_asset_models::{
            v2_fungible_asset_activities::{EventToCoinType, FungibleAssetActivity},
            v2_fungible_asset_balances::FungibleAssetBalance,
            v2_fungible_asset_utils::{
                ConcurrentFungibleAssetBalance, ConcurrentFungibleAssetSupply, FeeStatement,
                FungibleAssetMetadata, FungibleAssetStore, FungibleAssetSupply,
            },
        },
        object_models::v2_object_utils::{
            ObjectAggregatedData, ObjectAggregatedDataMapping, ObjectWithMetadata, Untransferable,
        },
        token_v2_models::v2_token_utils::{
            AptosCollection, ConcurrentSupply, FixedSupply, PropertyMapModel, TokenIdentifiers,
            TokenV2, TransferEvent, UnlimitedSupply,
        },
    },
    gap_detectors::ProcessingResult,
    utils::{
        counters::PROCESSOR_UNKNOWN_TYPE_COUNT,
        database::ArcDbPool,
        util::{get_entry_function_from_user_request, standardize_address},
    },
    IndexerGrpcProcessorConfig,
};
use ahash::AHashMap;
use aptos_protos::transaction::v1::{
    transaction::TxnData, write_set_change::Change, Event, Transaction, TransactionInfo,
    UserTransactionRequest,
};
use async_trait::async_trait;
use chrono::NaiveDateTime;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;

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
}

impl NightlyProcessor {
    pub fn new(connection_pool: ArcDbPool, config: NightlyProcessorConfig) -> Self {
        Self {
            connection_pool,
            config,
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

        parse_v2_token(&transactions);

        let processing_duration_in_secs = processing_start.elapsed().as_secs_f64();

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
    pub coin_changes: TransactionCoinChanges,
    pub token_changes: TransactionTokenChanges,
}

fn parse_v2_token(transactions: &[Transaction]) -> Vec<TransactionChanges> {
    let mut transaction_changes = vec![];

    for txn in transactions {
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
                    .with_label_values(&["FungibleAssetProcessor"])
                    .inc();
                continue;
            },
        };
        let transaction_info = txn.info.as_ref().expect("Transaction info doesn't exist!");
        let txn_timestamp = txn
            .timestamp
            .as_ref()
            .expect("Transaction timestamp doesn't exist!")
            .seconds;
        #[allow(deprecated)]
        let txn_timestamp =
            NaiveDateTime::from_timestamp_opt(txn_timestamp, 0).expect("Txn Timestamp is invalid!");

        let default = vec![];
        let (events, user_request, entry_function_id_str) = match txn_data {
            // TxnData::BlockMetadata(tx_inner) => (&tx_inner.events, None, None),
            // TxnData::Validator(tx_inner) => (&tx_inner.events, None, None),
            // TxnData::Genesis(tx_inner) => (&tx_inner.events, None, None),
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
        );

        let transaction_token_changes = process_transaction_tokens_changes(
            txn_version,
            transaction_info,
            txn_timestamp,
            events,
            &entry_function_id_str,
        );

        transaction_changes.push(TransactionChanges {
            txn_version,
            coin_changes: transaction_coin_changes,
            token_changes: transaction_token_changes,
        });
    }

    transaction_changes
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TransactionCoinChanges {
    pub txn_version: i64,
    pub asset_balances: Vec<FungibleAssetBalance>,
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
) -> TransactionCoinChanges {
    let mut fungible_asset_activities = vec![];
    let mut fungible_asset_balances = vec![];

    // Get Metadata for fungible assets by object
    let mut fungible_asset_object_helper: ObjectAggregatedDataMapping = AHashMap::new();

    // This is because v1 events (deposit/withdraw) don't have coin type so the only way is to match
    // the event to the resource using the event guid
    let mut event_to_v1_coin_type: EventToCoinType = AHashMap::new();

    // First loop to get all objects
    // Need to do a first pass to get all the objects
    for wsc in transaction_info.changes.iter() {
        if let Change::WriteResource(wr) = wsc.change.as_ref().unwrap() {
            if let Some(object) = ObjectWithMetadata::from_write_resource(wr, txn_version).unwrap()
            {
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
    // Loop to get the metadata relevant to parse v1 and v2.
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
                fungible_asset_balances.push(balance);
                // current_fungible_asset_balances
                //     .insert(current_balance.storage_id.clone(), current_balance.clone());
                event_to_v1_coin_type.extend(event_to_coin);
            }
            // Fill the v2 object metadata
            let address = standardize_address(&write_resource.address.to_string());
            if let Some(aggregated_data) = fungible_asset_object_helper.get_mut(&address) {
                if let Some(fungible_asset_metadata) =
                    FungibleAssetMetadata::from_write_resource(write_resource, txn_version).unwrap()
                {
                    aggregated_data.fungible_asset_metadata = Some(fungible_asset_metadata);
                }
                if let Some(fungible_asset_store) =
                    FungibleAssetStore::from_write_resource(write_resource, txn_version).unwrap()
                {
                    aggregated_data.fungible_asset_store = Some(fungible_asset_store);
                }
                if let Some(fungible_asset_supply) =
                    FungibleAssetSupply::from_write_resource(write_resource, txn_version).unwrap()
                {
                    aggregated_data.fungible_asset_supply = Some(fungible_asset_supply);
                }
                if let Some(concurrent_fungible_asset_supply) =
                    ConcurrentFungibleAssetSupply::from_write_resource(write_resource, txn_version)
                        .unwrap()
                {
                    aggregated_data.concurrent_fungible_asset_supply =
                        Some(concurrent_fungible_asset_supply);
                }
                if let Some(concurrent_fungible_asset_balance) =
                    ConcurrentFungibleAssetBalance::from_write_resource(write_resource, txn_version)
                        .unwrap()
                {
                    aggregated_data.concurrent_fungible_asset_balance =
                        Some(concurrent_fungible_asset_balance);
                }
                if let Some(untransferable) =
                    Untransferable::from_write_resource(write_resource, txn_version).unwrap()
                {
                    aggregated_data.untransferable = Some(untransferable);
                }
            }
        } else if let Change::DeleteResource(delete_resource) = wsc.change.as_ref().unwrap() {
            if let Some((balance, _current_balance, event_to_coin)) =
                FungibleAssetBalance::get_v1_from_delete_resource(
                    delete_resource,
                    index as i64,
                    txn_version,
                    txn_timestamp,
                )
                .unwrap()
            {
                fungible_asset_balances.push(balance);
                event_to_v1_coin_type.extend(event_to_coin);
            }
        }
    }

    // The artificial gas event, only need for v1
    if let Some(req) = user_request {
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
        fungible_asset_activities.push((gas_event, CoinAction::Gas));
    }

    // Loop to handle events and collect additional metadata from events for v2
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
            fungible_asset_activities.push((v2_activity, action));
        }
    }

    // Loop to handle all the other changes
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
                    fungible_asset_balances.push(balance);
                }
            },
            _ => {},
        }
    }

    TransactionCoinChanges {
        txn_version,
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
) -> TransactionTokenChanges {
    // Token V2 and V1 combined
    let mut token_activities_v2 = vec![];
    let mut token_v2_metadata_helper: ObjectAggregatedDataMapping = AHashMap::new();

    // println!("\n\ntransaction_info: {:#?}", transaction_info);

    // Need to do a first pass to get all the objects
    for wsc in transaction_info.changes.iter() {
        if let Change::WriteResource(wr) = wsc.change.as_ref().unwrap() {
            if let Some(object) = ObjectWithMetadata::from_write_resource(wr, txn_version).unwrap()
            {
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

    // Need to do a second pass to get all the structs related to the object
    for wsc in transaction_info.changes.iter() {
        if let Change::WriteResource(wr) = wsc.change.as_ref().unwrap() {
            let address = standardize_address(&wr.address.to_string());
            if let Some(aggregated_data) = token_v2_metadata_helper.get_mut(&address) {
                if let Some(fixed_supply) =
                    FixedSupply::from_write_resource(wr, txn_version).unwrap()
                {
                    aggregated_data.fixed_supply = Some(fixed_supply);
                }
                if let Some(unlimited_supply) =
                    UnlimitedSupply::from_write_resource(wr, txn_version).unwrap()
                {
                    aggregated_data.unlimited_supply = Some(unlimited_supply);
                }
                if let Some(aptos_collection) =
                    AptosCollection::from_write_resource(wr, txn_version).unwrap()
                {
                    aggregated_data.aptos_collection = Some(aptos_collection);
                }
                if let Some(property_map) =
                    PropertyMapModel::from_write_resource(wr, txn_version).unwrap()
                {
                    aggregated_data.property_map = Some(property_map);
                }
                if let Some(concurrent_supply) =
                    ConcurrentSupply::from_write_resource(wr, txn_version).unwrap()
                {
                    aggregated_data.concurrent_supply = Some(concurrent_supply);
                }
                if let Some(token) = TokenV2::from_write_resource(wr, txn_version).unwrap() {
                    aggregated_data.token = Some(token);
                }
                if let Some(fungible_asset_metadata) =
                    FungibleAssetMetadata::from_write_resource(wr, txn_version).unwrap()
                {
                    aggregated_data.fungible_asset_metadata = Some(fungible_asset_metadata);
                }
                if let Some(token_identifier) =
                    TokenIdentifiers::from_write_resource(wr, txn_version).unwrap()
                {
                    aggregated_data.token_identifier = Some(token_identifier);
                }
                if let Some(untransferable) =
                    Untransferable::from_write_resource(wr, txn_version).unwrap()
                {
                    aggregated_data.untransferable = Some(untransferable);
                }
            }
        }
    }

    // Pass through events to get the burn events and token activities v2
    // This needs to be here because we need the metadata above for token activities
    // and burn / transfer events need to come before the next section
    for (index, event) in events.iter().enumerate() {
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
        if let Some((event, token_action)) = TokenActivityV2::get_v1_from_parsed_event(
            event,
            txn_version,
            txn_timestamp,
            index as i64,
            &entry_function_id_str,
        )
        .unwrap()
        {
            token_activities_v2.push((event, token_action));
        }
        // handling all the token v2 events
        if let Some((event, token_action)) = TokenActivityV2::get_nft_v2_from_parsed_event(
            event,
            txn_version,
            txn_timestamp,
            index as i64,
            &entry_function_id_str,
            &token_v2_metadata_helper,
        )
        .unwrap()
        {
            token_activities_v2.push((event, token_action));
        }
    }

    return TransactionTokenChanges {
        txn_version,
        token_activities: token_activities_v2,
    };
}
