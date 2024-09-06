// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use super::{DefaultProcessingResult, ProcessorName, ProcessorTrait};
use crate::{
    db::common::models::{
        fungible_asset_models::v2_fungible_asset_utils::CoinAction,
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
            },
        },
    },
    nats_queue::{NatsQueueSender, WsPayload},
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
use ahash::{AHashMap, AHashSet};
use aptos_protos::transaction::v1::{
    transaction::TxnData, write_set_change::Change, Event, Transaction, TransactionInfo,
    UserTransactionRequest,
};
use async_trait::async_trait;
use bigdecimal::{BigDecimal, ToPrimitive};
use chrono::NaiveDateTime;
use core::panic;
use num::Zero;
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
use serde::{Deserialize, Serialize};
use std::{cmp::Ordering, collections::HashMap, fmt::Debug, sync::Arc};

const V1_GAS_COIN_TYPE: &str = "0x1::aptos_coin::AptosCoin";
const V2_GAS_COIN_TYPE: &str = "0x000000000000000000000000000000000000000000000000000000000000000a";

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
        // println!("Parsed transactions: {:#?}", parsed_transactions);

        let (ws_updates, notifications) =
            process_changes(start_version, end_version, parsed_transactions);

        let processing_duration_in_secs = processing_start.elapsed().as_secs_f64();

        println!(
            "Processed transactions from {} to {}, total {} in {} seconds",
            start_version,
            end_version,
            end_version - start_version,
            processing_duration_in_secs
        );

        // panic!("stop");
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

fn parse_v2_token(
    transactions: &[Transaction],
    table_handle: AHashMap<String, TableMetadataForToken>,
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
                    .with_label_values(&["NightlyProcessor"])
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

        let any_mint =
            transaction_coin_changes
                .asset_activities
                .iter()
                .any(|(activity, action)| {
                    action == &CoinAction::Freeze && activity.token_standard == TokenStandard::V1
                });

        if any_mint {
            println!("txn_version: {:#?}", txn_version);
            println!("transaction_coin_changes: {:#?}", transaction_coin_changes);
            println!(
                "transaction_token_changes: {:#?}",
                transaction_token_changes
            );

            panic!("stop");
        }

        // let any_mint =
        //     transaction_token_changes
        //         .token_activities
        //         .iter()
        //         .any(|(activity, action)| {
        //             action == &TokenAction::F && activity.token_standard == TokenStandard::V1
        //         });

        // let any_mint2 =
        //     transaction_token_changes
        //         .token_activities
        //         .iter()
        //         .any(|(activity, action)| {
        //             action == &TokenAction::Withdraw && activity.token_standard == TokenStandard::V1
        //         });

        // if any_mint && !any_mint2 {
        //     println!("txn_version: {:#?}", txn_version);
        //     println!("transaction_coin_changes: {:#?}", transaction_coin_changes);
        //     println!(
        //         "transaction_token_changes: {:#?}",
        //         transaction_token_changes
        //     );

        //     panic!("stop");
        // }

        // if txn_version == 1676396954 {
        //     println!("txn_version: {:#?}", txn_version);
        //     // println!("tx_info {:#?}", transaction_info);
        //     println!("transaction_coin_changes: {:#?}", transaction_coin_changes);
        //     println!(
        //         "transaction_token_changes: {:#?}",
        //         transaction_token_changes
        //     );

        //     panic!("stop");
        // }

        transaction_changes.push(TransactionChanges {
            txn_version,
            gas_used: transaction_info.gas_used as i128,
            coin_changes: transaction_coin_changes,
            token_changes: transaction_token_changes,
        });
    }

    transaction_changes
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TransactionCoinChanges {
    pub txn_version: i64,
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
                fungible_asset_balances
                    .entry(balance.owner_address.clone())
                    .or_insert_with(AHashMap::new)
                    .insert(balance.asset_type.clone(), balance);

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
                fungible_asset_balances
                    .entry(balance.owner_address.clone())
                    .or_insert_with(AHashMap::new)
                    .insert(balance.asset_type.clone(), balance);
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
            if action == CoinAction::Withdraw && v2_activity.amount == Some(txn_gas_used.clone()) {
                continue;
            }
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
                // if let Some((token_data, current_token_data)) =
                //     TokenDataV2::get_v1_from_write_table_item(
                //         table_item,
                //         txn_version,
                //         wsc_index,
                //         txn_timestamp,
                //     )
                //     .unwrap()
                // {
                //     token_datas_v2.push(token_data);
                //     current_token_datas_v2
                //         .insert(current_token_data.token_data_id.clone(), current_token_data);
                // }
                // if let Some(current_token_royalty) =
                //     CurrentTokenRoyaltyV1::get_v1_from_write_table_item(
                //         table_item,
                //         txn_version,
                //         txn_timestamp,
                //     )
                //     .unwrap()
                // {
                //     current_token_royalties_v1.insert(
                //         current_token_royalty.token_data_id.clone(),
                //         current_token_royalty,
                //     );
                // }
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
                    // token_ownerships_v2.push(token_ownership);
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
                    // token_ownerships_v2.push(token_ownership);
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
                // if let Some((collection, current_collection)) =
                //     CollectionV2::get_v2_from_write_resource(
                //         resource,
                //         txn_version,
                //         wsc_index,
                //         txn_timestamp,
                //         &token_v2_metadata_helper,
                //     )
                //     .unwrap()
                // {
                //     collections_v2.push(collection);
                //     current_collections_v2
                //         .insert(current_collection.collection_id.clone(), current_collection);
                // }
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
                    // token_ownerships_v2.append(&mut ownerships);
                    current_token_ownerships_v2.extend(current_ownerships);
                    // token_datas_v2.push(token_data);
                    // current_token_datas_v2
                    //     .insert(current_token_data.token_data_id.clone(), current_token_data);
                }

                // // Add burned NFT handling for token datas (can probably be merged with below)
                // if let Some(deleted_token_data) =
                //     TokenDataV2::get_burned_nft_v2_from_write_resource(
                //         resource,
                //         txn_version,
                //         txn_timestamp,
                //         &tokens_burned,
                //     )
                //     .unwrap()
                // {
                //     current_deleted_token_datas_v2
                //         .insert(deleted_token_data.token_data_id.clone(), deleted_token_data);
                // }
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
                //     // token_ownerships_v2.push(nft_ownership);
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

                // // Track token properties
                // if let Some(token_metadata) = CurrentTokenV2Metadata::from_write_resource(
                //     resource,
                //     txn_version,
                //     &token_v2_metadata_helper,
                // )
                // .unwrap()
                // {
                //     current_token_v2_metadata.insert(
                //         (
                //             token_metadata.object_address.clone(),
                //             token_metadata.resource_type.clone(),
                //         ),
                //         token_metadata,
                //     );
                // }
            },
            Change::DeleteResource(resource) => {
                // // Add burned NFT handling for token datas (can probably be merged with below)
                // if let Some(deleted_token_data) =
                //     TokenDataV2::get_burned_nft_v2_from_delete_resource(
                //         resource,
                //         txn_version,
                //         txn_timestamp,
                //         &tokens_burned,
                //     )
                //     .unwrap()
                // {
                //     current_deleted_token_datas_v2
                //         .insert(deleted_token_data.token_data_id.clone(), deleted_token_data);
                // }
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

    // let any_mint = token_activities_v2.iter().any(|(activity, action)| {
    //     action == &TokenAction::Withdraw && activity.token_standard == TokenStandard::V1
    //     // && token_activities_v2.len() == 1
    // });

    // if any_mint {
    //     println!("txn_version: {:#?}", txn_version);
    //     println!("Prior NFT Ownership: {:#?}", prior_nft_ownership);
    //     println!(
    //         "Current Token Ownerships V2: {:#?}",
    //         current_token_ownerships_v2
    //     );
    //     println!("Current burned token datas: {:#?}", tokens_burned);
    //     println!("Current minted token datas: {:#?}", tokens_minted);
    //     println!(
    //         "Current Deleted Token Ownerships V2: {:#?}",
    //         current_deleted_token_ownerships_v2
    //     );
    //     // println!("transaction_coin_changes: {:#?}", token_activities_v2);
    //     println!("transaction_token_changes: {:#?}", token_activities_v2);

    //     panic!("stop");
    // }

    return TransactionTokenChanges {
        txn_version,
        token_activities: token_activities_v2,
    };
}

// Helper struct to catch transfer between two users for v1 as direct transfer event data sucks
#[derive(Debug, Serialize, Deserialize)]
struct TokenEventData {
    from_address: Option<String>,
    to_address: Option<String>,
    token_data_id: String,
    event_index: i64,
}

fn process_changes(
    start_tx: u64,
    end_tx: u64,
    transactions: Vec<TransactionChanges>,
) -> (WsPayload, Vec<(u64, Vec<AptosIndexerNotification>)>) {
    let mut ws_updates: Vec<(u64, Vec<AptosWsApiMsg>)> = vec![];
    let mut notifications: Vec<(u64, Vec<AptosIndexerNotification>)> = vec![];

    for tx in transactions.iter() {
        let tx_version = tx.txn_version;
        let _gas_used = tx.gas_used;
        let coin_changes = &tx.coin_changes;
        let token_changes = &tx.token_changes;

        let mut ws_transaction_account_coin_updates: AHashMap<String, AptosCoinBalanceUpdate> =
            AHashMap::new();
        let mut ws_account_token_updates: AHashMap<String, AptosAccountTokensUpdate> =
            AHashMap::new();

        if tx_version != 1682620014 {
            // println!("txn_version: {:#?}", tx_version);
            // println!("coin_changes: {:#?}", coin_changes);
            // println!("token_changes: {:#?}", token_changes);

            // panic!("stop");
            continue;
        }

        // if tx_version == 1682379246 {
        //     println!("txn_version: {:#?}", tx_version);
        //     println!("coin_changes: {:#?}", coin_changes);
        //     println!("token_changes: {:#?}", token_changes);

        //     panic!("stop");
        //     // continue;
        // }

        // println!("coin_changes: {:#?}", coin_changes);

        for (asset_activity, action) in coin_changes.asset_activities.iter() {
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
            let coin_balance = match coin_changes
                .asset_balances
                .get(&owner_address)
                .and_then(|balances| balances.get(&coin_type))
            {
                Some(coin_balance) => coin_balance,
                None => {
                    tracing::warn!(
                        transaction_version = tx_version,
                        "Coin balance doesn't exist"
                    );
                    PROCESSOR_UNKNOWN_TYPE_COUNT
                        .with_label_values(&["NightlyProcessor"])
                        .inc();
                    continue;
                },
            };

            // Parse the activity amount and balance amount
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
                    continue;
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
                    continue;
                },
            };

            let object_status = if action == &CoinAction::Gas {
                AptosCoinObjectUpdateStatus::Mutated(CoinMutated {
                    change: -activity_amount,
                })
            } else {
                match coin_balance_amount.cmp(&activity_amount) {
                    Ordering::Less => {
                        // Balance decreased, so it was mutated
                        AptosCoinObjectUpdateStatus::Mutated(CoinMutated {
                            change: -activity_amount,
                        })
                    },
                    Ordering::Greater => {
                        let update = match action {
                            CoinAction::Deposit => {
                                // Balance increased, so it was deposited
                                AptosCoinObjectUpdateStatus::Mutated(CoinMutated {
                                    change: activity_amount,
                                })
                            },
                            CoinAction::Withdraw => {
                                // Balance decreased, so it was withdrawn
                                AptosCoinObjectUpdateStatus::Mutated(CoinMutated {
                                    change: -activity_amount,
                                })
                            },

                            _ => {
                                continue;
                            },
                        };

                        update
                    },
                    Ordering::Equal => {
                        if coin_balance_amount.is_zero() {
                            // If balance is 0 after the transaction, it's deleted
                            AptosCoinObjectUpdateStatus::Deleted(CoinDeleted {
                                amount: activity_amount.abs(),
                            })
                        } else if coin_balance_amount == activity_amount {
                            // Balance equals the activity amount, so it was created
                            AptosCoinObjectUpdateStatus::Created(CoinCreated {
                                amount: activity_amount,
                            })
                        } else {
                            // Check if the event is for freezing or unfreezing
                            match action {
                                CoinAction::Freeze => AptosCoinObjectUpdateStatus::Frozen,
                                CoinAction::UnFreeze => AptosCoinObjectUpdateStatus::Unfrozen,
                                _ => {
                                    continue;
                                },
                            }
                        }
                    },
                }
            };

            // println!("txn_version: {:#?}", tx_version);
            // println!("coin_type: {:#?}", coin_type);
            // println!("owner_address: {:#?}", owner_address);
            // println!("activity_amount: {:#?}", activity_amount);
            // println!("coin_balance_amount: {:#?}", coin_balance_amount);
            // println!("object_status: {:#?}", object_status);

            ws_transaction_account_coin_updates
                .entry(owner_address.clone())
                .or_insert_with(|| AptosCoinBalanceUpdate {
                    aptos_address: owner_address,
                    changed_balances: HashMap::new(),
                    sequence_number: tx_version as u64,
                    timestamp_ms: chrono::Utc::now().naive_utc().and_utc().timestamp_millis()
                        as u64,
                })
                .changed_balances
                .entry(coin_type.clone())
                .or_insert(vec![])
                .push(CoinUpdate {
                    coin_type,
                    current_total_balance: coin_balance_amount,
                    standard: match asset_activity.token_standard {
                        TokenStandard::V1 => AptosCoinStandard::Coin,
                        TokenStandard::V2 => AptosCoinStandard::FungibleAsset,
                    },
                    status: object_status,
                });
        }

        // Create a map to track unmatched deposits and withdrawals
        let mut pending_events: HashMap<String, TokenEventData> = HashMap::new();
        // Create a map to track token claims and offers
        let mut claim_offer_map: HashMap<String, AptosObjectUpdateStatus> = HashMap::new();

        // let mut user_changes: HashMap<String, AptosAccountTokensUpdate> = HashMap::new();
        for (token_activity, action) in token_changes.token_activities.iter() {
            let token_data_id = token_activity.token_data_id.clone();
            let event_id = token_activity.event_index.to_string().clone();

            match action {
                TokenAction::Mint => {
                    let address = match &token_activity.from_address {
                        Some(address) => address,
                        None => {
                            tracing::warn!(
                                transaction_version = tx_version,
                                "From address doesn't exist"
                            );
                            PROCESSOR_UNKNOWN_TYPE_COUNT
                                .with_label_values(&["NightlyProcessor"])
                                .inc();
                            continue;
                        },
                    };

                    let user_tokens_update_entry = ws_account_token_updates
                        .entry(address.clone())
                        .or_insert_with(|| AptosAccountTokensUpdate {
                            aptos_address: address.clone(),
                            tokens_changes: HashMap::new(),
                            sequence_number: tx_version as u64,
                            timestamp_ms: chrono::Utc::now()
                                .naive_utc()
                                .and_utc()
                                .timestamp_millis()
                                as u64,
                        })
                        .tokens_changes
                        .entry(token_data_id.clone())
                        .or_insert_with(Vec::new);

                    user_tokens_update_entry.push(AptosTokenChangeUpdate {
                        token_id: token_data_id.clone(),
                        event_id: event_id.clone(),
                        status: AptosObjectUpdateStatus::Created,
                        standard: match token_activity.token_standard {
                            TokenStandard::V1 => AptosTokenStandard::Token,
                            TokenStandard::V2 => AptosTokenStandard::DigitalAsset,
                        },
                    });
                },
                TokenAction::Burn => {
                    // It is quite complicated, basically we don't have database to lookup for the token ownership, well fuck
                    // Instead we will save change under the event owner address
                    let address = &token_activity.event_account_address;

                    let user_tokens_update_entry = ws_account_token_updates
                        .entry(address.clone())
                        .or_insert_with(|| AptosAccountTokensUpdate {
                            aptos_address: address.clone(),
                            tokens_changes: HashMap::new(),
                            sequence_number: tx_version as u64,
                            timestamp_ms: chrono::Utc::now()
                                .naive_utc()
                                .and_utc()
                                .timestamp_millis()
                                as u64,
                        })
                        .tokens_changes
                        .entry(token_data_id.clone())
                        .or_insert_with(Vec::new);

                    user_tokens_update_entry.push(AptosTokenChangeUpdate {
                        token_id: token_data_id.clone(),
                        event_id: event_id.clone(),
                        status: AptosObjectUpdateStatus::Deleted,
                        standard: match token_activity.token_standard {
                            TokenStandard::V1 => AptosTokenStandard::Token,
                            TokenStandard::V2 => AptosTokenStandard::DigitalAsset,
                        },
                    });
                },
                TokenAction::Transfer => {
                    let from_user_address = match &token_activity.from_address {
                        Some(address) => address,
                        None => {
                            tracing::warn!(
                                transaction_version = tx_version,
                                "From address doesn't exist"
                            );
                            PROCESSOR_UNKNOWN_TYPE_COUNT
                                .with_label_values(&["NightlyProcessor"])
                                .inc();
                            continue;
                        },
                    };

                    let to_user_address = match &token_activity.to_address {
                        Some(address) => address,
                        None => {
                            tracing::warn!(
                                transaction_version = tx_version,
                                "To address doesn't exist"
                            );
                            PROCESSOR_UNKNOWN_TYPE_COUNT
                                .with_label_values(&["NightlyProcessor"])
                                .inc();
                            continue;
                        },
                    };

                    let from_user_tokens_update_entry = ws_account_token_updates
                        .entry(from_user_address.clone())
                        .or_insert_with(|| AptosAccountTokensUpdate {
                            aptos_address: from_user_address.clone(),
                            tokens_changes: HashMap::new(),
                            sequence_number: tx_version as u64,
                            timestamp_ms: chrono::Utc::now()
                                .naive_utc()
                                .and_utc()
                                .timestamp_millis()
                                as u64,
                        })
                        .tokens_changes
                        .entry(token_data_id.clone())
                        .or_insert_with(Vec::new);

                    from_user_tokens_update_entry.push(AptosTokenChangeUpdate {
                        token_id: token_data_id.clone(),
                        event_id: event_id.clone(),
                        status: AptosObjectUpdateStatus::Sent(Sent {
                            sender_address: from_user_address.clone(),
                            receiver_address: to_user_address.clone(),
                        }),
                        standard: match token_activity.token_standard {
                            TokenStandard::V1 => AptosTokenStandard::Token,
                            TokenStandard::V2 => AptosTokenStandard::DigitalAsset,
                        },
                    });

                    let to_user_tokens_update_entry = ws_account_token_updates
                        .entry(to_user_address.clone())
                        .or_insert_with(|| AptosAccountTokensUpdate {
                            aptos_address: to_user_address.clone(),
                            tokens_changes: HashMap::new(),
                            sequence_number: tx_version as u64,
                            timestamp_ms: chrono::Utc::now()
                                .naive_utc()
                                .and_utc()
                                .timestamp_millis()
                                as u64,
                        })
                        .tokens_changes
                        .entry(token_data_id.clone())
                        .or_insert_with(Vec::new);

                    to_user_tokens_update_entry.push(AptosTokenChangeUpdate {
                        token_id: token_data_id.clone(),
                        event_id: event_id.clone(),
                        status: AptosObjectUpdateStatus::Received(Received {
                            sender_address: from_user_address.clone(),
                            receiver_address: to_user_address.clone(),
                        }),
                        standard: match token_activity.token_standard {
                            TokenStandard::V1 => AptosTokenStandard::Token,
                            TokenStandard::V2 => AptosTokenStandard::DigitalAsset,
                        },
                    });
                },
                TokenAction::Mutate => {
                    let address = match &token_activity.from_address {
                        Some(address) => address,
                        None => {
                            tracing::warn!(
                                transaction_version = tx_version,
                                "From address doesn't exist"
                            );
                            PROCESSOR_UNKNOWN_TYPE_COUNT
                                .with_label_values(&["NightlyProcessor"])
                                .inc();
                            continue;
                        },
                    };

                    let user_tokens_update_entry = ws_account_token_updates
                        .entry(address.clone())
                        .or_insert_with(|| AptosAccountTokensUpdate {
                            aptos_address: address.clone(),
                            tokens_changes: HashMap::new(),
                            sequence_number: tx_version as u64,
                            timestamp_ms: chrono::Utc::now()
                                .naive_utc()
                                .and_utc()
                                .timestamp_millis()
                                as u64,
                        })
                        .tokens_changes
                        .entry(token_data_id.clone())
                        .or_insert_with(Vec::new);

                    user_tokens_update_entry.push(AptosTokenChangeUpdate {
                        token_id: token_data_id.clone(),
                        event_id: event_id.clone(),
                        status: AptosObjectUpdateStatus::Mutated,
                        standard: match token_activity.token_standard {
                            TokenStandard::V1 => AptosTokenStandard::Token,
                            TokenStandard::V2 => AptosTokenStandard::DigitalAsset,
                        },
                    });
                },
                // Old v1 token actions
                TokenAction::Offer => {
                    let owner_address = match &token_activity.from_address {
                        Some(address) => address,
                        None => {
                            tracing::warn!(
                                transaction_version = tx_version,
                                "Owner address doesn't exist"
                            );
                            PROCESSOR_UNKNOWN_TYPE_COUNT
                                .with_label_values(&["NightlyProcessor"])
                                .inc();
                            continue;
                        },
                    };

                    let receiver_address = match &token_activity.to_address {
                        Some(address) => address,
                        None => {
                            tracing::warn!(
                                transaction_version = tx_version,
                                "Receiver address doesn't exist"
                            );
                            PROCESSOR_UNKNOWN_TYPE_COUNT
                                .with_label_values(&["NightlyProcessor"])
                                .inc();
                            continue;
                        },
                    };

                    // Add the offer to the claim_offer_map
                    claim_offer_map.insert(
                        token_data_id.clone(),
                        AptosObjectUpdateStatus::Offer(Offer {
                            sender_address: owner_address.clone(),
                            receiver_address: receiver_address.clone(),
                        }),
                    );

                    let user_tokens_update_entry = ws_account_token_updates
                        .entry(owner_address.clone())
                        .or_insert_with(|| AptosAccountTokensUpdate {
                            aptos_address: owner_address.clone(),
                            tokens_changes: HashMap::new(),
                            sequence_number: tx_version as u64,
                            timestamp_ms: chrono::Utc::now()
                                .naive_utc()
                                .and_utc()
                                .timestamp_millis()
                                as u64,
                        })
                        .tokens_changes
                        .entry(token_data_id.clone())
                        .or_insert_with(Vec::new);

                    user_tokens_update_entry.push(AptosTokenChangeUpdate {
                        token_id: token_data_id.clone(),
                        event_id: event_id.clone(),
                        status: AptosObjectUpdateStatus::Offer(Offer {
                            sender_address: owner_address.clone(),
                            receiver_address: receiver_address.clone(),
                        }),
                        standard: match token_activity.token_standard {
                            TokenStandard::V1 => AptosTokenStandard::Token,
                            TokenStandard::V2 => AptosTokenStandard::DigitalAsset,
                        },
                    });

                    let receiver_tokens_update_entry = ws_account_token_updates
                        .entry(receiver_address.clone())
                        .or_insert_with(|| AptosAccountTokensUpdate {
                            aptos_address: receiver_address.clone(),
                            tokens_changes: HashMap::new(),
                            sequence_number: tx_version as u64,
                            timestamp_ms: chrono::Utc::now()
                                .naive_utc()
                                .and_utc()
                                .timestamp_millis()
                                as u64,
                        })
                        .tokens_changes
                        .entry(token_data_id.clone())
                        .or_insert_with(Vec::new);

                    receiver_tokens_update_entry.push(AptosTokenChangeUpdate {
                        token_id: token_data_id.clone(),
                        event_id: event_id.clone(),
                        status: AptosObjectUpdateStatus::PendingClaim(PendingClaim {
                            sender_address: owner_address.clone(),
                            receiver_address: receiver_address.clone(),
                        }),
                        standard: match token_activity.token_standard {
                            TokenStandard::V1 => AptosTokenStandard::Token,
                            TokenStandard::V2 => AptosTokenStandard::DigitalAsset,
                        },
                    });
                },
                TokenAction::CancelClaim => {
                    // Kinda unused event in v1, handle it somehow
                    let owner_address = match &token_activity.from_address {
                        Some(address) => address,
                        None => {
                            tracing::warn!(
                                transaction_version = tx_version,
                                "Owner address doesn't exist"
                            );
                            PROCESSOR_UNKNOWN_TYPE_COUNT
                                .with_label_values(&["NightlyProcessor"])
                                .inc();
                            continue;
                        },
                    };

                    let user_tokens_update_entry = ws_account_token_updates
                        .entry(owner_address.clone())
                        .or_insert_with(|| AptosAccountTokensUpdate {
                            aptos_address: owner_address.clone(),
                            tokens_changes: HashMap::new(),
                            sequence_number: tx_version as u64,
                            timestamp_ms: chrono::Utc::now()
                                .naive_utc()
                                .and_utc()
                                .timestamp_millis()
                                as u64,
                        })
                        .tokens_changes
                        .entry(token_data_id.clone())
                        .or_insert_with(Vec::new);

                    user_tokens_update_entry.push(AptosTokenChangeUpdate {
                        token_id: token_data_id.clone(),
                        event_id: event_id.clone(),
                        status: AptosObjectUpdateStatus::CancelClaim,
                        standard: match token_activity.token_standard {
                            TokenStandard::V1 => AptosTokenStandard::Token,
                            TokenStandard::V2 => AptosTokenStandard::DigitalAsset,
                        },
                    });
                },
                TokenAction::Claim => {
                    let receiver_address = match &token_activity.to_address {
                        Some(address) => address,
                        None => {
                            tracing::warn!(
                                transaction_version = tx_version,
                                "Receiver address doesn't exist"
                            );
                            PROCESSOR_UNKNOWN_TYPE_COUNT
                                .with_label_values(&["NightlyProcessor"])
                                .inc();
                            continue;
                        },
                    };

                    // Add the claim to the claim_offer_map
                    claim_offer_map.insert(token_data_id.clone(), AptosObjectUpdateStatus::Claim);

                    let receiver_tokens_update_entry = ws_account_token_updates
                        .entry(receiver_address.clone())
                        .or_insert_with(|| AptosAccountTokensUpdate {
                            aptos_address: receiver_address.clone(),
                            tokens_changes: HashMap::new(),
                            sequence_number: tx_version as u64,
                            timestamp_ms: chrono::Utc::now()
                                .naive_utc()
                                .and_utc()
                                .timestamp_millis()
                                as u64,
                        })
                        .tokens_changes
                        .entry(token_data_id.clone())
                        .or_insert_with(Vec::new);

                    receiver_tokens_update_entry.push(AptosTokenChangeUpdate {
                        token_id: token_data_id.clone(),
                        event_id: event_id.clone(),
                        status: AptosObjectUpdateStatus::Claim,
                        standard: match token_activity.token_standard {
                            TokenStandard::V1 => AptosTokenStandard::Token,
                            TokenStandard::V2 => AptosTokenStandard::DigitalAsset,
                        },
                    });
                },

                TokenAction::Deposit => {
                    pending_events
                        .entry(token_data_id.clone())
                        .or_insert(TokenEventData {
                            from_address: token_activity.from_address.clone(),
                            to_address: token_activity.to_address.clone(),
                            token_data_id: token_data_id.clone(),
                            event_index: token_activity.event_index,
                        })
                        .to_address = token_activity.to_address.clone();
                },
                TokenAction::Withdraw => {
                    pending_events
                        .entry(token_data_id.clone())
                        .or_insert(TokenEventData {
                            from_address: token_activity.from_address.clone(),
                            to_address: token_activity.to_address.clone(),
                            token_data_id: token_data_id.clone(),
                            event_index: token_activity.event_index,
                        })
                        .from_address = token_activity.from_address.clone();
                },
            }
        }

        // Process "Transfer", deposit and withdrawal events from v1
        for (token_data_id, event_data) in pending_events.iter() {
            match (&event_data.from_address, &event_data.to_address) {
                (Some(from), Some(to)) => {
                    // Both from_address and to_address are present, so it's a transfer
                    let from_user_tokens_update_entry = ws_account_token_updates
                        .entry(from.clone())
                        .or_insert_with(|| AptosAccountTokensUpdate {
                            aptos_address: from.clone(),
                            tokens_changes: HashMap::new(),
                            sequence_number: tx_version as u64,
                            timestamp_ms: chrono::Utc::now().timestamp_millis() as u64,
                        })
                        .tokens_changes
                        .entry(token_data_id.clone())
                        .or_insert_with(Vec::new);

                    from_user_tokens_update_entry.push(AptosTokenChangeUpdate {
                        token_id: token_data_id.clone(),
                        event_id: event_data.event_index.to_string(),
                        status: AptosObjectUpdateStatus::Sent(Sent {
                            sender_address: from.clone(),
                            receiver_address: to.clone(),
                        }),
                        standard: AptosTokenStandard::Token,
                    });

                    let to_user_tokens_update_entry = ws_account_token_updates
                        .entry(to.clone())
                        .or_insert_with(|| AptosAccountTokensUpdate {
                            aptos_address: to.clone(),
                            tokens_changes: HashMap::new(),
                            sequence_number: tx_version as u64,
                            timestamp_ms: chrono::Utc::now().timestamp_millis() as u64,
                        })
                        .tokens_changes
                        .entry(token_data_id.clone())
                        .or_insert_with(Vec::new);

                    to_user_tokens_update_entry.push(AptosTokenChangeUpdate {
                        token_id: token_data_id.clone(),
                        event_id: event_data.event_index.to_string(),
                        status: AptosObjectUpdateStatus::Received(Received {
                            sender_address: from.clone(),
                            receiver_address: to.clone(),
                        }),
                        standard: AptosTokenStandard::Token,
                    });
                },
                (Some(withdrawal_address), None) => {
                    let user_tokens_update_entry = ws_account_token_updates
                        .entry(withdrawal_address.clone())
                        .or_insert_with(|| AptosAccountTokensUpdate {
                            aptos_address: withdrawal_address.clone(),
                            tokens_changes: HashMap::new(),
                            sequence_number: tx_version as u64,
                            timestamp_ms: chrono::Utc::now().timestamp_millis() as u64,
                        })
                        .tokens_changes
                        .entry(token_data_id.clone())
                        .or_insert_with(Vec::new);

                    // Check if the token_id is in claim_offer_map and has an "Offer" status
                    let is_offer_present =
                        claim_offer_map.get(token_data_id).map_or(false, |status| {
                            matches!(status, AptosObjectUpdateStatus::Offer(_))
                        });

                    if is_offer_present {
                        continue;
                    }

                    user_tokens_update_entry.push(AptosTokenChangeUpdate {
                        token_id: token_data_id.clone(),
                        event_id: event_data.event_index.to_string(),
                        status: AptosObjectUpdateStatus::Deleted,
                        standard: AptosTokenStandard::Token,
                    });
                },
                (None, Some(deposit_address)) => {
                    let user_tokens_update_entry = ws_account_token_updates
                        .entry(deposit_address.clone())
                        .or_insert_with(|| AptosAccountTokensUpdate {
                            aptos_address: deposit_address.clone(),
                            tokens_changes: HashMap::new(),
                            sequence_number: tx_version as u64,
                            timestamp_ms: chrono::Utc::now().timestamp_millis() as u64,
                        })
                        .tokens_changes
                        .entry(token_data_id.clone())
                        .or_insert_with(Vec::new);

                    // Check if the token_id is in claim_offer_map and has an "Offer" status
                    let is_claim_present =
                        claim_offer_map.get(token_data_id).map_or(false, |status| {
                            matches!(status, AptosObjectUpdateStatus::Claim)
                        });

                    if is_claim_present {
                        continue;
                    }

                    user_tokens_update_entry.push(AptosTokenChangeUpdate {
                        token_id: token_data_id.clone(),
                        event_id: event_data.event_index.to_string(),
                        status: AptosObjectUpdateStatus::Created,
                        standard: AptosTokenStandard::Token,
                    });
                },
                (None, None) => {
                    // Both addresses are missing, log this as a warning
                    tracing::warn!(
                        "Event for token_data_id {} has neither from_address nor to_address",
                        token_data_id
                    );
                },
            }
        }

        let mut transaction_notifications = Vec::new();

        // Before we send the updates, we will prepare notifications
        for (account_address, update) in ws_transaction_account_coin_updates.iter() {
            let mut aggregated_changes: HashMap<String, i128> = HashMap::new();

            // Aggregate changes by coin_type
            for (coin_type, balance_updates) in update.changed_balances.iter() {
                if coin_type == V1_GAS_COIN_TYPE || coin_type == V2_GAS_COIN_TYPE {
                    // Skip gas coins
                    continue;
                }

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
                            transaction_notifications.push(AptosIndexerNotification::CoinFrozen(
                                CoinFrozen {
                                    aptos_address: account_address.clone(),
                                    coin_type: coin_type.clone(),
                                    frozen: true,
                                },
                            ));
                        },
                        AptosCoinObjectUpdateStatus::Unfrozen => {
                            transaction_notifications.push(AptosIndexerNotification::CoinFrozen(
                                CoinFrozen {
                                    aptos_address: account_address.clone(),
                                    coin_type: coin_type.clone(),
                                    frozen: false,
                                },
                            ));
                        },
                    }
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
                    transaction_notifications.push(AptosIndexerNotification::CoinSent(CoinSent {
                        sender_address: account_address.clone(),
                        coin_type,
                        amount,
                    }));
                }
            } else if only_positive && !received.is_empty() {
                // If only positive changes, it's a CoinReceived event
                for (coin_type, amount) in received {
                    transaction_notifications.push(AptosIndexerNotification::CoinReceived(
                        CoinReceived {
                            receiver_address: account_address.clone(),
                            coin_type,
                            amount,
                        },
                    ));
                }
            } else if !spent.is_empty() && !received.is_empty() {
                // If there are both negative and positive changes, it's a CoinSwap event
                transaction_notifications.push(AptosIndexerNotification::CoinSwap(CoinSwap {
                    aptos_address: account_address.clone(),
                    spent,
                    received,
                }));
            }
        }

        // println!("ws_account_token_updates {:#?}", ws_account_token_updates);

        for (_account_address, token_update) in ws_account_token_updates.iter() {
            for (_token_id, token_changes) in token_update.tokens_changes.iter() {
                for token_change in token_changes.iter() {
                    match &token_change.status {
                        AptosObjectUpdateStatus::Created => {
                            transaction_notifications.push(AptosIndexerNotification::NftMinted(
                                NftMinted {
                                    aptos_address: token_update.aptos_address.clone(),
                                    nft_id: token_change.token_id.clone(),
                                },
                            ));
                        },
                        AptosObjectUpdateStatus::Offer(offer) => {
                            transaction_notifications.push(AptosIndexerNotification::NftOffer(
                                NftOffer {
                                    sender: offer.sender_address.clone(),
                                    receiver: offer.receiver_address.clone(),
                                    nft_id: token_change.token_id.clone(),
                                },
                            ));
                        },
                        AptosObjectUpdateStatus::PendingClaim(_) => {
                            // We only need offer, gonna split it at alexandria level
                            // skip case for now
                        },
                        AptosObjectUpdateStatus::Claim => {
                            transaction_notifications.push(AptosIndexerNotification::NftClaim(
                                NftClaim {
                                    receiver: token_update.aptos_address.clone(),
                                    nft_id: token_change.token_id.clone(),
                                },
                            ));
                        },
                        AptosObjectUpdateStatus::CancelClaim => {
                            transaction_notifications.push(
                                AptosIndexerNotification::NftCancelClaim(NftCancelClaim {
                                    aptos_address: token_update.aptos_address.clone(),
                                    nft_id: token_change.token_id.clone(),
                                }),
                            );
                        },
                        AptosObjectUpdateStatus::Mutated => {
                            // skip case for now
                        },
                        AptosObjectUpdateStatus::Deleted => {
                            transaction_notifications.push(AptosIndexerNotification::NftBurned(
                                NftBurned {
                                    aptos_address: token_update.aptos_address.clone(),
                                    nft_id: token_change.token_id.clone(),
                                },
                            ));
                        },
                        AptosObjectUpdateStatus::Sent(sent) => {
                            transaction_notifications.push(AptosIndexerNotification::NftSent(
                                NftSent {
                                    sender_address: sent.sender_address.clone(),
                                    nft_id: token_change.token_id.clone(),
                                },
                            ));
                        },
                        AptosObjectUpdateStatus::Received(received) => {
                            transaction_notifications.push(AptosIndexerNotification::NftReceived(
                                NftReceived {
                                    receiver_address: received.receiver_address.clone(),
                                    nft_id: token_change.token_id.clone(),
                                },
                            ));
                        },
                    }
                }
            }
        }

        ws_updates.push((
            tx_version as u64,
            ws_transaction_account_coin_updates
                .into_iter()
                .map(|(_, v)| AptosWsApiMsg::AptosCoinBalanceUpdate(v))
                .chain(
                    ws_account_token_updates
                        .into_iter()
                        .map(|(_, v)| AptosWsApiMsg::AptosAccountTokensUpdate(v)),
                )
                .collect(),
        ));

        notifications.push((tx_version as u64, transaction_notifications));

        // if tx_version == 1682620014 {
        //     println!("txn_version: {:#?}", tx_version);
        //     // println!("coin_changes: {:#?}", coin_changes);
        //     // println!("token_changes: {:#?}", token_changes);
        //     // println!("ws_account_token_updates: {:#?}", ws_updates);
        //     println!("notifications: {:#?}", notifications);

        //     panic!("stop");
        // }
    }

    ((start_tx, end_tx, ws_updates), notifications)
}
