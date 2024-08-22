// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use super::{DefaultProcessingResult, ProcessorName, ProcessorTrait};
use crate::{
    db::common::models::{
        fungible_asset_models::v2_fungible_asset_utils::FungibleAssetMetadata,
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
    write_set_change::Change, EntryFunctionPayload, Transaction, TransactionPayload,
};
use async_trait::async_trait;
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
    for txn in transactions {
        println!("txn: {:?}", txn.version);
        println!("txn_data: {:?}", txn.txn_data);
        let txn_version = txn.version;
        let txn_data = match txn.txn_data.as_ref() {
            Some(data) => data,
            None => {
                PROCESSOR_UNKNOWN_TYPE_COUNT
                    .with_label_values(&["TokenV2Processor"])
                    .inc();
                tracing::warn!(
                    transaction_version = txn_version,
                    "Transaction data doesn't exist"
                );
                continue;
            },
        };
        let txn_version = txn.version as i64;
        let txn_timestamp = parse_timestamp(txn.timestamp.as_ref().unwrap(), txn_version);
        let transaction_info = txn.info.as_ref().expect("Transaction info doesn't exist!");

        if let TxnData::User(user_txn) = txn_data {
            if let Some(request) = user_txn.request.as_ref() {
                if let Some(payload) = request.payload.as_ref() {
                    if let Some(payload) = payload.payload.as_ref() {
                        match payload {
                            Payload::EntryFunctionPayload(EntryFunctionPayload {
                                function,
                                type_arguments,
                                arguments,
                                entry_function_id_str,
                            }) => {
                                println!("function: {:?}", function);
                                println!("type_arguments: {:?}", type_arguments);
                                for argument in type_arguments {
                                    if let Some(content) = &argument.content {
                                        match content {
                                            Content::Struct(tag) => {
                                                println!("token : {:?}", tag.address);
                                            },
                                            _ => {},
                                        }
                                    }
                                }

                                println!("arguments: {:?}", arguments);
                                println!("entry_function_id_str: {:?}", entry_function_id_str);
                            },
                            _ => {},
                        }
                    }
                }
            }

            let user_request = user_txn
                .request
                .as_ref()
                .expect("Sends is not present in user txn");
            let entry_function_id_str = get_entry_function_from_user_request(user_request);

            // Pass through events to get the burn events and token activities v2
            // This needs to be here because we need the metadata above for token activities
            // and burn / transfer events need to come before the next section
            for (index, event) in user_txn.events.iter().enumerate() {
                println!("event: {:?}", event);
            }
        }
    }
}
