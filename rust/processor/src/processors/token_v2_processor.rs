// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use super::{DefaultProcessingResult, ProcessorName, ProcessorTrait};
use crate::{
    db::common::models::{
        coin_models::{
            coin_activities::CoinActivity,
            coin_balances::{CoinBalance, CurrentCoinBalance},
            coin_infos::CoinInfo,
            coin_utils::CoinEvent,
        },
        fungible_asset_models::{
            v2_fungible_asset_activities::{CoinType, CurrentCoinBalancePK, EventToCoinType},
            v2_fungible_asset_utils::{FeeStatement, FungibleAssetMetadata},
        },
        object_models::v2_object_utils::{
            ObjectAggregatedData, ObjectAggregatedDataMapping, ObjectWithMetadata, Untransferable,
        },
        token_models::{
            token_claims::CurrentTokenPendingClaim,
            tokens::{CurrentTokenPendingClaimPK, TableHandleToOwner, TableMetadataForToken},
        },
        token_v2_models::{
            v1_token_royalty::CurrentTokenRoyaltyV1,
            v2_collections::{CollectionV2, CurrentCollectionV2, CurrentCollectionV2PK},
            v2_token_activities::TokenActivityV2,
            v2_token_datas::{CurrentTokenDataV2, CurrentTokenDataV2PK, TokenDataV2},
            v2_token_metadata::{CurrentTokenV2Metadata, CurrentTokenV2MetadataPK},
            v2_token_ownerships::{
                CurrentTokenOwnershipV2, CurrentTokenOwnershipV2PK, NFTOwnershipV2,
                TokenOwnershipV2,
            },
            v2_token_utils::{
                AptosCollection, Burn, BurnEvent, ConcurrentSupply, FixedSupply, MintEvent,
                PropertyMapModel, TokenIdentifiers, TokenV2, TokenV2Burned, TokenV2Minted,
                TransferEvent, UnlimitedSupply,
            },
        },
    },
    gap_detectors::ProcessingResult,
    schema,
    utils::{
        counters::PROCESSOR_UNKNOWN_TYPE_COUNT,
        database::{execute_in_chunks, get_config_table_chunk_size, ArcDbPool, DbPoolConnection},
        util::{get_entry_function_from_user_request, parse_timestamp, standardize_address},
    },
    worker::TableFlags,
    IndexerGrpcProcessorConfig,
};
use ahash::{AHashMap, AHashSet};
use anyhow::bail;
use aptos_protos::transaction::v1::{
    move_type::Content, transaction::TxnData, transaction_payload::Payload,
    write_set_change::Change, EntryFunctionPayload, Event, Transaction, TransactionPayload,
    UserTransactionRequest,
};
use async_trait::async_trait;
use chrono::NaiveDateTime;
use diesel::{
    pg::{upsert::excluded, Pg},
    query_builder::QueryFragment,
    ExpressionMethods,
};
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use tracing::error;

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

        let mut conn = self.get_conn().await;

        let query_retries = self.config.query_retries;
        let query_retry_delay_ms = self.config.query_retry_delay_ms;
        parse_v2_token(&transactions).await;
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

async fn parse_v2_token(transactions: &[Transaction]) {
    // Code above is inefficient (multiple passthroughs) so I'm approaching TokenV2 with a cleaner code structure
    for transaction in transactions {
        // println!("\n\nTXN HERE: {:?}", transaction.version);
        // println!("txn_data: {:?}", transaction.txn_data);
        // All the items we want to track
        let mut coin_activities = Vec::new();
        let mut coin_balances = Vec::new();
        let mut coin_infos: AHashMap<CoinType, CoinInfo> = AHashMap::new();
        let mut current_coin_balances: AHashMap<CurrentCoinBalancePK, CurrentCoinBalance> =
            AHashMap::new();
        // This will help us get the coin type when we see coin deposit/withdraw events for coin activities
        let mut all_event_to_coin_type: EventToCoinType = AHashMap::new();

        // Extracts events and user request from genesis and user transactions. Other transactions won't have coin events
        let txn_data = match transaction.txn_data.as_ref() {
            Some(data) => data,
            None => {
                PROCESSOR_UNKNOWN_TYPE_COUNT
                    .with_label_values(&["CoinActivity"])
                    .inc();
                tracing::warn!(
                    transaction_version = transaction.version,
                    "Transaction data doesn't exist",
                );
                return Default::default();
            },
        };
        let (events, maybe_user_request): (&Vec<Event>, Option<&UserTransactionRequest>) =
            match txn_data {
                TxnData::Genesis(inner) => (&inner.events, None),
                TxnData::User(inner) => (&inner.events, inner.request.as_ref()),
                _ => return Default::default(),
            };

        // The rest are fields common to all transactions
        let txn_version = transaction.version as i64;
        let block_height = transaction.block_height as i64;
        let transaction_info = transaction
            .info
            .as_ref()
            .expect("Transaction info doesn't exist!");
        let txn_timestamp = transaction
            .timestamp
            .as_ref()
            .expect("Transaction timestamp doesn't exist!")
            .seconds;
        #[allow(deprecated)]
        let txn_timestamp =
            NaiveDateTime::from_timestamp_opt(txn_timestamp, 0).expect("Txn Timestamp is invalid!");

        // Handling gas first
        let mut entry_function_id_str = None;
        if let Some(user_request) = maybe_user_request {
            let fee_statement = events.iter().find_map(|event| {
                let event_type = event.type_str.as_str();
                FeeStatement::from_event(event_type, &event.data, txn_version)
            });

            entry_function_id_str = get_entry_function_from_user_request(user_request);
            coin_activities.push(CoinActivity::get_gas_event(
                transaction_info,
                user_request,
                &entry_function_id_str,
                txn_version,
                txn_timestamp,
                block_height,
                fee_statement,
            ));
        }

        // Need coin info from move resources
        for wsc in &transaction_info.changes {
            let (maybe_coin_info, maybe_coin_balance_data) =
                if let Change::WriteResource(write_resource) = &wsc.change.as_ref().unwrap() {
                    (
                        CoinInfo::from_write_resource(write_resource, txn_version, txn_timestamp)
                            .unwrap(),
                        CoinBalance::from_write_resource(
                            write_resource,
                            txn_version,
                            txn_timestamp,
                        )
                        .unwrap(),
                    )
                } else {
                    (None, None)
                };

            if let Some(coin_info) = maybe_coin_info {
                coin_infos.insert(coin_info.coin_type.clone(), coin_info);
            }
            if let Some((coin_balance, current_coin_balance, event_to_coin_type)) =
                maybe_coin_balance_data
            {
                current_coin_balances.insert(
                    (
                        coin_balance.owner_address.clone(),
                        coin_balance.coin_type.clone(),
                    ),
                    current_coin_balance,
                );
                coin_balances.push(coin_balance);
                all_event_to_coin_type.extend(event_to_coin_type);
            }
        }
        for (index, event) in events.iter().enumerate() {
            let event_type = event.type_str.clone();
            if let Some(parsed_event) =
                CoinEvent::from_event(event_type.as_str(), &event.data, txn_version).unwrap()
            {
                coin_activities.push(CoinActivity::from_parsed_event(
                    &event_type,
                    event,
                    &parsed_event,
                    txn_version,
                    &all_event_to_coin_type,
                    block_height,
                    &entry_function_id_str,
                    txn_timestamp,
                    index as i64,
                ));
            };
        }

        println!("\n\ncoin_activities: {:?}", coin_activities);
        println!("coin_balances: {:?}", coin_balances);
        println!("coin_infos: {:?}", coin_infos);
        println!("current_coin_balances: {:?}\n", current_coin_balances);

        for (index, event) in events.iter().enumerate() {
            println!("event: {:?}", event);
        }

        println!("\n\n\n");

        panic!("stop");
    }
}
