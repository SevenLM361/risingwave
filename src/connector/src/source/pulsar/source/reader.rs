// Copyright 2023 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, ensure, Result};
use async_trait::async_trait;
use futures::StreamExt;
use futures_async_stream::try_stream;
use icelake::io::FileScanStream;
use icelake::Table;
use itertools::Itertools;
use pulsar::consumer::InitialPosition;
use pulsar::message::proto::MessageIdData;
use pulsar::{Consumer, ConsumerBuilder, ConsumerOptions, Pulsar, SubType, TokioExecutor};

use crate::parser::ParserConfig;
use crate::source::pulsar::split::PulsarSplit;
use crate::source::pulsar::{PulsarEnumeratorOffset, PulsarProperties};
use crate::source::{
    into_chunk_stream, BoxSourceWithStateStream, Column, CommonSplitReader, SourceContextRef,
    SourceMessage, SourceMessages, SplitId, SplitMetaData, SplitReader,
};

pub enum PulsarSplitReader {
    Broker(PulsarBrokerReader),
    Iceberg(PulsarIcebergReader),
}

/// This reader reads from pulsar broker
pub struct PulsarBrokerReader {
    pulsar: Pulsar<TokioExecutor>,
    consumer: Consumer<Vec<u8>, TokioExecutor>,
    split: PulsarSplit,

    split_id: SplitId,
    parser_config: ParserConfig,
    source_ctx: SourceContextRef,
}

// {ledger_id}:{entry_id}:{partition}:{batch_index}
fn parse_message_id(id: &str) -> Result<MessageIdData> {
    let splits = id.split(':').collect_vec();

    if splits.len() < 2 || splits.len() > 4 {
        return Err(anyhow!("illegal message id string {}", id));
    }

    let ledger_id = splits[0]
        .parse::<u64>()
        .map_err(|e| anyhow!("illegal ledger id {}", e))?;
    let entry_id = splits[1]
        .parse::<u64>()
        .map_err(|e| anyhow!("illegal entry id {}", e))?;

    let mut message_id = MessageIdData {
        ledger_id,
        entry_id,
        partition: None,
        batch_index: None,
        ack_set: vec![],
        batch_size: None,
        first_chunk_message_id: None,
    };

    if splits.len() > 2 {
        let partition = splits[2]
            .parse::<i32>()
            .map_err(|e| anyhow!("illegal partition {}", e))?;
        message_id.partition = Some(partition);
    }

    if splits.len() == 4 {
        let batch_index = splits[3]
            .parse::<i32>()
            .map_err(|e| anyhow!("illegal batch index {}", e))?;
        message_id.batch_index = Some(batch_index);
    }

    Ok(message_id)
}

#[async_trait]
impl SplitReader for PulsarBrokerReader {
    type Properties = PulsarProperties;
    type Split = PulsarSplit;

    async fn new(
        props: PulsarProperties,
        splits: Vec<PulsarSplit>,
        parser_config: ParserConfig,
        source_ctx: SourceContextRef,
        _columns: Option<Vec<Column>>,
    ) -> Result<Self> {
        ensure!(splits.len() == 1, "only support single split");
        let split = splits.into_iter().next().unwrap();
        let pulsar = props.common.build_client().await?;
        let topic = split.topic.to_string();

        tracing::debug!("creating consumer for pulsar split topic {}", topic,);

        let builder: ConsumerBuilder<TokioExecutor> = pulsar
            .consumer()
            .with_topic(&topic)
            .with_subscription_type(SubType::Exclusive)
            .with_subscription(format!(
                "consumer-{}",
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_micros()
            ));

        let builder = match split.start_offset.clone() {
            PulsarEnumeratorOffset::Earliest => {
                if topic.starts_with("non-persistent://") {
                    tracing::warn!("Earliest offset is not supported for non-persistent topic, use Latest instead");
                    builder.with_options(
                        ConsumerOptions::default().with_initial_position(InitialPosition::Latest),
                    )
                } else {
                    builder.with_options(
                        ConsumerOptions::default().with_initial_position(InitialPosition::Earliest),
                    )
                }
            }
            PulsarEnumeratorOffset::Latest => builder.with_options(
                ConsumerOptions::default().with_initial_position(InitialPosition::Latest),
            ),
            PulsarEnumeratorOffset::MessageId(m) => {
                if topic.starts_with("non-persistent://") {
                    tracing::warn!("MessageId offset is not supported for non-persistent topic, use Latest instead");
                    builder.with_options(
                        ConsumerOptions::default().with_initial_position(InitialPosition::Latest),
                    )
                } else {
                    builder.with_options(pulsar::ConsumerOptions {
                        durable: Some(false),
                        start_message_id: parse_message_id(m.as_str()).ok(),
                        ..Default::default()
                    })
                }
            }

            PulsarEnumeratorOffset::Timestamp(_) => builder,
        };

        let consumer: Consumer<Vec<u8>, _> = builder.build().await?;
        if let PulsarEnumeratorOffset::Timestamp(_ts) = split.start_offset {
            // FIXME: Here we need pulsar-rs to support the send + sync consumer
            // consumer
            //     .seek(None, None, Some(ts as u64), pulsar.clone())
            //     .await?;
        }

        Ok(Self {
            pulsar,
            consumer,
            split_id: split.id(),
            split,
            parser_config,
            source_ctx,
        })
    }

    fn into_stream(self) -> BoxSourceWithStateStream {
        let parser_config = self.parser_config.clone();
        let source_context = self.source_ctx.clone();
        into_chunk_stream(self, parser_config, source_context)
    }
}

impl CommonSplitReader for PulsarBrokerReader {
    #[try_stream(ok = Vec<SourceMessage>, error = anyhow::Error)]
    async fn into_data_stream(self) {
        let max_chunk_size = self.source_ctx.source_ctrl_opts.chunk_size;
        #[for_await]
        for msgs in self.consumer.ready_chunks(max_chunk_size) {
            let mut res = Vec::with_capacity(msgs.len());
            for msg in msgs {
                let msg = SourceMessage::from(msg?);
                res.push(msg);
            }
            yield res;
        }
    }
}

/// Read history data from iceberg table
pub struct PulsarIcebergReader {
    split: PulsarSplit,
    source_ctx: SourceContextRef,
}

impl PulsarIcebergReader {
    fn new(split: PulsarSplit, source_ctx: SourceContextRef) -> Self {
        Self { split, source_ctx }
    }

    async fn scan(&self) -> Result<FileScanStream> {
        let table = self.create_iceberg_table().await?;

        let max_chunk_size = self.source_ctx.source_ctrl_opts.chunk_size;

        // TODO: Add partition
        Ok(table
            .new_scan_builder()
            .with_batch_size(max_chunk_size)
            .build()?
            .scan(&table)
            .await?)
    }

    async fn create_iceberg_table(&self) -> Result<Table> {
        todo!()
    }

    #[try_stream(ok = Vec<SourceMessage>, error = anyhow::Error)]
    async fn into_data_stream(self) {
        #[for_await]
        for file_scan in self.scan().await? {
            let file_scan = file_scan?;

            #[for_await]
            for record_batch in file_scan.scan().await? {
                let batch = record_batch?;
                let msgs = SourceMessages::try_from((&batch, &self.split.topic))?.0;
                yield msgs;
            }
        }
    }
}
