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

use anyhow::anyhow;
use arrow_array::RecordBatch;
use pulsar::consumer::Message;

use crate::error::ConnectorError;
use crate::source::pulsar::topic::Topic;
use crate::source::{SourceMessage, SourceMessages, SourceMeta};

impl From<Message<Vec<u8>>> for SourceMessage {
    fn from(msg: Message<Vec<u8>>) -> Self {
        let message_id = msg.message_id.id;

        SourceMessage {
            key: msg.payload.metadata.partition_key.clone().map(|k| k.into()),
            payload: Some(msg.payload.data),
            offset: format!(
                "{}:{}:{}:{}",
                message_id.ledger_id,
                message_id.entry_id,
                message_id.partition.unwrap_or(-1),
                message_id.batch_index.unwrap_or(-1)
            ),
            split_id: msg.topic.into(),
            meta: SourceMeta::Empty,
        }
    }
}

/// Meta columns from pulsar's iceberg table
const META_COLUMN_TOPIC: &str = "_topic";
const META_COLUMN_KEY: &str = "_key";
const META_COLUMN_LEDGER_ID: &str = "_ledgerId";
const META_COLUMN_ENTRY_ID: &str = "_entryId";
const META_COLUMN_BATCH_INDEX: &str = "_batchIndex";
const META_COLUMN_PARTITION: &str = "_partition";

impl TryFrom<(&RecordBatch, &Topic)> for SourceMessages {
    type Error = ConnectorError;

    fn try_from(value: (&RecordBatch, &Topic)) -> Result<Self, ConnectorError> {
        let (batch, topic) = value;

        let mut ret = Vec::with_capacity(batch.num_rows());
        let jsons = arrow_json::writer::record_batches_to_json_rows(&[batch]).map_err(|e| {
            ConnectorError::Pulsar(anyhow!("Failed to convert record batch to json: {}", e))
        })?;
        for json in jsons {
            let source_message = SourceMessage {
                key: json
                    .get(META_COLUMN_KEY)
                    .and_then(|v| v.as_str())
                    .map(|v| v.as_bytes().to_vec()),
                payload: Some(
                    serde_json::to_string(&json)
                        .map_err(|e| {
                            ConnectorError::Pulsar(anyhow!("Failed to serialize json: {}", e))
                        })?
                        .into_bytes(),
                ),
                offset: format!(
                    "{}:{}:{}:{}",
                    json.get(META_COLUMN_LEDGER_ID)
                        .and_then(|v| v.as_i64())
                        .ok_or_else(|| ConnectorError::Pulsar(anyhow!(
                            "Ledger id not found in iceberg table"
                        )))?,
                    json.get(META_COLUMN_ENTRY_ID)
                        .and_then(|v| v.as_i64())
                        .ok_or_else(|| ConnectorError::Pulsar(anyhow!(
                            "Entry id not found in iceberg table"
                        )))?,
                    json.get(META_COLUMN_PARTITION)
                        .and_then(|v| v.as_i64())
                        .unwrap_or(-1),
                    json.get(META_COLUMN_BATCH_INDEX)
                        .and_then(|v| v.as_i64())
                        .unwrap_or(-1)
                ),
                split_id: topic.to_string().into(),
                meta: SourceMeta::Empty,
            };
            ret.push(source_message);
        }

        Ok(SourceMessages(ret))
    }
}
