// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use super::{DefaultProcessingResult, ProcessorName, ProcessorTrait};
use crate::db::common::models::fungible_asset_models::v2_fungible_asset_utils::CoinAction;
use crate::db::common::models::token_models::token_claims::CurrentTokenPendingClaim;
use crate::db::common::models::token_models::tokens::{
    CurrentTokenPendingClaimPK, TableMetadataForToken,
};
use crate::db::common::models::token_v2_models::v1_token_royalty::CurrentTokenRoyaltyV1;
use crate::db::common::models::token_v2_models::v2_collections::{
    CollectionV2, CurrentCollectionV2, CurrentCollectionV2PK,
};
use crate::db::common::models::token_v2_models::v2_token_activities::TokenActivityV2;
use crate::db::common::models::token_v2_models::v2_token_datas::{
    CurrentTokenDataV2, CurrentTokenDataV2PK, TokenDataV2,
};
use crate::db::common::models::token_v2_models::v2_token_metadata::{
    CurrentTokenV2Metadata, CurrentTokenV2MetadataPK,
};
use crate::db::common::models::token_v2_models::v2_token_ownerships::{
    CurrentTokenOwnershipV2, CurrentTokenOwnershipV2PK, NFTOwnershipV2, TokenOwnershipV2,
};
use crate::db::common::models::token_v2_models::v2_token_utils::{
    AptosCollection, Burn, BurnEvent, ConcurrentSupply, FixedSupply, MintEvent, PropertyMapModel,
    TokenIdentifiers, TokenV2, TokenV2Burned, TokenV2Minted, TransferEvent, UnlimitedSupply,
};
use crate::IndexerGrpcProcessorConfig;
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
    },
    gap_detectors::ProcessingResult,
    utils::{
        counters::PROCESSOR_UNKNOWN_TYPE_COUNT,
        database::ArcDbPool,
        util::{get_entry_function_from_user_request, standardize_address},
    },
    worker::TableFlags,
};
use ahash::{AHashMap, AHashSet};
use aptos_protos::transaction::v1::{transaction::TxnData, write_set_change::Change, Transaction};
use aptos_protos::transaction::v1::{
    Event, TransactionInfo, UserTransaction, UserTransactionRequest,
};
use async_trait::async_trait;
use chrono::NaiveDateTime;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TokenV2ProcessorConfig {
    #[serde(default = "IndexerGrpcProcessorConfig::default_query_retries")]
    pub query_retries: u32,
    #[serde(default = "IndexerGrpcProcessorConfig::default_query_retry_delay_ms")]
    pub query_retry_delay_ms: u64,
}

pub struct TokenV2Processor {
    connection_pool: ArcDbPool,
    config: TokenV2ProcessorConfig,
    per_table_chunk_sizes: AHashMap<String, usize>,
    deprecated_tables: TableFlags,
}

impl TokenV2Processor {
    pub fn new(
        connection_pool: ArcDbPool,
        config: TokenV2ProcessorConfig,
        per_table_chunk_sizes: AHashMap<String, usize>,
        deprecated_tables: TableFlags,
    ) -> Self {
        Self {
            connection_pool,
            config,
            per_table_chunk_sizes,
            deprecated_tables,
        }
    }
}

impl Debug for TokenV2Processor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let state = &self.connection_pool.state();
        write!(
            f,
            "TokenV2TransactionProcessor {{ connections: {:?}  idle_connections: {:?} }}",
            state.connections, state.idle_connections
        )
    }
}

#[async_trait]
impl ProcessorTrait for TokenV2Processor {
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

        parse_v2_token(&transactions, table_handle_to_owner).await;
        // Token V2 processing which includes token v1
        // let (
        //     mut collections_v2,
        //     mut token_datas_v2,
        //     mut token_ownerships_v2,
        //     current_collections_v2,
        //     current_token_datas_v2,
        //     current_deleted_token_datas_v2,
        //     current_token_ownerships_v2,
        //     current_deleted_token_ownerships_v2,
        //     token_activities_v2,
        //     mut current_token_v2_metadata,
        //     current_token_royalties_v1,
        //     current_token_claims,
        // ) = parse_v2_token(
        //     &transactions,
        //     &table_handle_to_owner,
        //     &mut conn,
        //     query_retries,
        //     query_retry_delay_ms,
        // )
        // .await;

        let processing_duration_in_secs = processing_start.elapsed().as_secs_f64();
        let db_insertion_start = std::time::Instant::now();

        let db_insertion_duration_in_secs = db_insertion_start.elapsed().as_secs_f64();
        Ok(ProcessingResult::DefaultProcessingResult(
            DefaultProcessingResult {
                start_version,
                end_version,
                processing_duration_in_secs,
                db_insertion_duration_in_secs,
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
}

async fn parse_v2_token(
    transactions: &[Transaction],
    table_handle_to_owner: AHashMap<String, TableMetadataForToken>,
) -> Vec<TransactionChanges> {
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

        // let transaction_coin_changes = process_transaction_coins_changes(
        //     txn_version,
        //     block_height,
        //     transaction_info,
        //     txn_timestamp,
        //     events,
        //     &user_request.cloned(),
        //     &entry_function_id_str,
        // );

        // // Check if any token activity is about frozing a token
        // let is_frozen = transaction_coin_changes
        //     .asset_activities
        //     .iter()
        //     .any(|activity| activity.0.is_frozen.unwrap_or(false) == true);
        // if !transaction_coin_changes.asset_activities.is_empty() && is_frozen {
        //     println!(
        //         "\n\ntxn_version: {}, block height {}",
        //         txn_version, block_height
        //     );
        //     println!("{:#?}", transaction_coin_changes);
        //     panic!("STOP");
        // }

        // transaction_changes.push(TransactionChanges {
        //     txn_version,
        //     coin_changes: transaction_coin_changes,
        // });

        let transaction_token_changes = process_transaction_tokens_changes(
            txn_version,
            transaction_info,
            txn_timestamp,
            events,
            &user_request.cloned(),
            &entry_function_id_str,
            &table_handle_to_owner,
        );
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
    pub asset_balances: Vec<FungibleAssetBalance>,
    pub asset_activities: Vec<(FungibleAssetActivity, CoinAction)>,
}

fn process_transaction_tokens_changes(
    txn_version: i64,
    transaction_info: &TransactionInfo,
    txn_timestamp: NaiveDateTime,
    events: &Vec<Event>,
    user_request: &Option<UserTransactionRequest>,
    entry_function_id_str: &Option<String>,
    table_handle_to_owner: &AHashMap<String, TableMetadataForToken>,
) {
    // Token V2 and V1 combined
    let mut collections_v2 = vec![];
    let mut token_datas_v2 = vec![];
    let mut token_ownerships_v2 = vec![];
    let mut token_activities_v2 = vec![];

    let mut current_collections_v2: AHashMap<CurrentCollectionV2PK, CurrentCollectionV2> =
        AHashMap::new();
    let mut current_token_datas_v2: AHashMap<CurrentTokenDataV2PK, CurrentTokenDataV2> =
        AHashMap::new();
    let mut current_deleted_token_datas_v2: AHashMap<CurrentTokenDataV2PK, CurrentTokenDataV2> =
        AHashMap::new();
    let mut current_token_ownerships_v2: AHashMap<
        CurrentTokenOwnershipV2PK,
        CurrentTokenOwnershipV2,
    > = AHashMap::new();
    let mut current_deleted_token_ownerships_v2 = AHashMap::new();
    // Tracks prior ownership in case a token gets burned
    let mut prior_nft_ownership: AHashMap<String, NFTOwnershipV2> = AHashMap::new();
    // Get Metadata for token v2 by object
    // We want to persist this through the entire batch so that even if a token is burned,
    // we can still get the object core metadata for it
    let mut token_v2_metadata_helper: ObjectAggregatedDataMapping = AHashMap::new();
    // Basically token properties
    let mut current_token_v2_metadata: AHashMap<CurrentTokenV2MetadataPK, CurrentTokenV2Metadata> =
        AHashMap::new();
    let mut current_token_royalties_v1: AHashMap<CurrentTokenDataV2PK, CurrentTokenRoyaltyV1> =
        AHashMap::new();
    // migrating this from v1 token model as we don't have any replacement table for this
    let mut all_current_token_claims: AHashMap<
        CurrentTokenPendingClaimPK,
        CurrentTokenPendingClaim,
    > = AHashMap::new();

    // Get burn events for token v2 by object
    let mut tokens_burned: TokenV2Burned = AHashMap::new();

    // Get mint events for token v2 by object
    let mut tokens_minted: TokenV2Minted = AHashSet::new();

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
        println!("event: {:?}", event);
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

    for (index, wsc) in transaction_info.changes.iter().enumerate() {
        let wsc_index = index as i64;
        match wsc.change.as_ref().unwrap() {
            Change::WriteTableItem(table_item) => {
                // if let Some((collection, current_collection)) =
                //     CollectionV2::get_v1_from_write_table_item(
                //         table_item,
                //         txn_version,
                //         wsc_index,
                //         txn_timestamp,
                //         table_handle_to_owner,
                //         conn,
                //         query_retries,
                //         query_retry_delay_ms,
                //     )
                //     .await
                //     .unwrap()
                // {
                //     collections_v2.push(collection);
                //     current_collections_v2
                //         .insert(current_collection.collection_id.clone(), current_collection);
                // }
                if let Some((token_data, current_token_data)) =
                    TokenDataV2::get_v1_from_write_table_item(
                        table_item,
                        txn_version,
                        wsc_index,
                        txn_timestamp,
                    )
                    .unwrap()
                {
                    token_datas_v2.push(token_data);
                    current_token_datas_v2
                        .insert(current_token_data.token_data_id.clone(), current_token_data);
                }
                if let Some(current_token_royalty) =
                    CurrentTokenRoyaltyV1::get_v1_from_write_table_item(
                        table_item,
                        txn_version,
                        txn_timestamp,
                    )
                    .unwrap()
                {
                    current_token_royalties_v1.insert(
                        current_token_royalty.token_data_id.clone(),
                        current_token_royalty,
                    );
                }
                if let Some((token_ownership, current_token_ownership)) =
                    TokenOwnershipV2::get_v1_from_write_table_item(
                        table_item,
                        txn_version,
                        wsc_index,
                        txn_timestamp,
                        table_handle_to_owner,
                    )
                    .unwrap()
                {
                    token_ownerships_v2.push(token_ownership);
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
                if let Some((token_ownership, current_token_ownership)) =
                    TokenOwnershipV2::get_v1_from_delete_table_item(
                        table_item,
                        txn_version,
                        wsc_index,
                        txn_timestamp,
                        table_handle_to_owner,
                    )
                    .unwrap()
                {
                    token_ownerships_v2.push(token_ownership);
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
                if let Some((collection, current_collection)) =
                    CollectionV2::get_v2_from_write_resource(
                        resource,
                        txn_version,
                        wsc_index,
                        txn_timestamp,
                        &token_v2_metadata_helper,
                    )
                    .unwrap()
                {
                    collections_v2.push(collection);
                    current_collections_v2
                        .insert(current_collection.collection_id.clone(), current_collection);
                }
                if let Some((token_data, current_token_data)) =
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
                    let (mut ownerships, current_ownerships) =
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
                    token_ownerships_v2.append(&mut ownerships);
                    current_token_ownerships_v2.extend(current_ownerships);
                    token_datas_v2.push(token_data);
                    current_token_datas_v2
                        .insert(current_token_data.token_data_id.clone(), current_token_data);
                }

                // Add burned NFT handling for token datas (can probably be merged with below)
                if let Some(deleted_token_data) =
                    TokenDataV2::get_burned_nft_v2_from_write_resource(
                        resource,
                        txn_version,
                        txn_timestamp,
                        &tokens_burned,
                    )
                    .unwrap()
                {
                    current_deleted_token_datas_v2
                        .insert(deleted_token_data.token_data_id.clone(), deleted_token_data);
                }
                // Add burned NFT handling
                // if let Some((nft_ownership, current_nft_ownership)) =
                //     TokenOwnershipV2::get_burned_nft_v2_from_write_resource(
                //         resource,
                //         txn_version,
                //         wsc_index,
                //         txn_timestamp,
                //         &prior_nft_ownership,
                //         &tokens_burned,
                //         &token_v2_metadata_helper,
                //         conn,
                //         query_retries,
                //         query_retry_delay_ms,
                //     )
                //     .await
                //     .unwrap()
                // {
                //     token_ownerships_v2.push(nft_ownership);
                //     prior_nft_ownership.insert(
                //         current_nft_ownership.token_data_id.clone(),
                //         NFTOwnershipV2 {
                //             token_data_id: current_nft_ownership.token_data_id.clone(),
                //             owner_address: current_nft_ownership.owner_address.clone(),
                //             is_soulbound: current_nft_ownership.is_soulbound_v2,
                //         },
                //     );
                //     current_deleted_token_ownerships_v2.insert(
                //         (
                //             current_nft_ownership.token_data_id.clone(),
                //             current_nft_ownership.property_version_v1.clone(),
                //             current_nft_ownership.owner_address.clone(),
                //             current_nft_ownership.storage_id.clone(),
                //         ),
                //         current_nft_ownership,
                //     );
                // }

                // Track token properties
                if let Some(token_metadata) = CurrentTokenV2Metadata::from_write_resource(
                    resource,
                    txn_version,
                    &token_v2_metadata_helper,
                )
                .unwrap()
                {
                    current_token_v2_metadata.insert(
                        (
                            token_metadata.object_address.clone(),
                            token_metadata.resource_type.clone(),
                        ),
                        token_metadata,
                    );
                }
            },
            Change::DeleteResource(resource) => {
                // Add burned NFT handling for token datas (can probably be merged with below)
                if let Some(deleted_token_data) =
                    TokenDataV2::get_burned_nft_v2_from_delete_resource(
                        resource,
                        txn_version,
                        txn_timestamp,
                        &tokens_burned,
                    )
                    .unwrap()
                {
                    current_deleted_token_datas_v2
                        .insert(deleted_token_data.token_data_id.clone(), deleted_token_data);
                }
                // if let Some((nft_ownership, current_nft_ownership)) =
                //     TokenOwnershipV2::get_burned_nft_v2_from_delete_resource(
                //         resource,
                //         txn_version,
                //         wsc_index,
                //         txn_timestamp,
                //         &prior_nft_ownership,
                //         &tokens_burned,
                //         conn,
                //         query_retries,
                //         query_retry_delay_ms,
                //     )
                //     .await
                //     .unwrap()
                // {
                //     token_ownerships_v2.push(nft_ownership);
                //     prior_nft_ownership.insert(
                //         current_nft_ownership.token_data_id.clone(),
                //         NFTOwnershipV2 {
                //             token_data_id: current_nft_ownership.token_data_id.clone(),
                //             owner_address: current_nft_ownership.owner_address.clone(),
                //             is_soulbound: current_nft_ownership.is_soulbound_v2,
                //         },
                //     );
                //     current_deleted_token_ownerships_v2.insert(
                //         (
                //             current_nft_ownership.token_data_id.clone(),
                //             current_nft_ownership.property_version_v1.clone(),
                //             current_nft_ownership.owner_address.clone(),
                //             current_nft_ownership.storage_id.clone(),
                //         ),
                //         current_nft_ownership,
                //     );
                // }
            },
            _ => {},
        }
    }

    // Getting list of values and sorting by pk in order to avoid postgres deadlock since we're doing multi threaded db writes
    let mut current_collections_v2 = current_collections_v2
        .into_values()
        .collect::<Vec<CurrentCollectionV2>>();
    let mut current_token_datas_v2 = current_token_datas_v2
        .into_values()
        .collect::<Vec<CurrentTokenDataV2>>();
    let mut current_deleted_token_datas_v2 = current_deleted_token_datas_v2
        .into_values()
        .collect::<Vec<CurrentTokenDataV2>>();
    let mut current_token_ownerships_v2 = current_token_ownerships_v2
        .into_values()
        .collect::<Vec<CurrentTokenOwnershipV2>>();
    let mut current_token_v2_metadata = current_token_v2_metadata
        .into_values()
        .collect::<Vec<CurrentTokenV2Metadata>>();
    let mut current_deleted_token_ownerships_v2 = current_deleted_token_ownerships_v2
        .into_values()
        .collect::<Vec<CurrentTokenOwnershipV2>>();
    let mut current_token_royalties_v1 = current_token_royalties_v1
        .into_values()
        .collect::<Vec<CurrentTokenRoyaltyV1>>();
    let mut all_current_token_claims = all_current_token_claims
        .into_values()
        .collect::<Vec<CurrentTokenPendingClaim>>();
    // Sort by PK
    current_collections_v2.sort_by(|a, b| a.collection_id.cmp(&b.collection_id));
    current_deleted_token_datas_v2.sort_by(|a, b| a.token_data_id.cmp(&b.token_data_id));
    current_token_datas_v2.sort_by(|a, b| a.token_data_id.cmp(&b.token_data_id));
    current_token_ownerships_v2.sort();
    current_token_v2_metadata.sort();
    current_deleted_token_ownerships_v2.sort();
    current_token_royalties_v1.sort();
    all_current_token_claims.sort();

    if !token_activities_v2.is_empty() && token_activities_v2.len() > 3 {
        println!("\n\ncollections_v2: {:#?}", collections_v2);
        println!("token_activities_v2: {:#?}", token_activities_v2);
        println!("token_datas_v2: {:#?}", token_datas_v2);
        println!("token_ownerships_v2: {:#?}", token_ownerships_v2);
        println!("current_collections_v2: {:#?}", current_collections_v2);
        println!("current_token_datas_v2: {:#?}", current_token_datas_v2);
        println!(
            "current_token_ownerships_v2: {:#?}",
            current_token_ownerships_v2
        );
        println!(
            "current_token_v2_metadata: {:#?}",
            current_token_v2_metadata
        );
        println!(
            "current_deleted_token_ownerships_v2: {:#?}",
            current_deleted_token_ownerships_v2
        );
        println!(
            "current_token_royalties_v1: {:#?}",
            current_token_royalties_v1
        );
        println!("all_current_token_claims: {:#?}", all_current_token_claims);

        panic!("STOP");
    }

    // (
    //     collections_v2,
    //     token_datas_v2,
    //     token_ownerships_v2,
    //     current_collections_v2,
    //     current_token_datas_v2,
    //     current_deleted_token_datas_v2,
    //     current_token_ownerships_v2,
    //     current_deleted_token_ownerships_v2,
    //     token_activities_v2,
    //     current_token_v2_metadata,
    //     current_token_royalties_v1,
    //     all_current_token_claims,
    // )
}
