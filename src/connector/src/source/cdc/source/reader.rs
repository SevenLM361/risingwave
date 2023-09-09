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

use std::str::FromStr;

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use futures::pin_mut;
use futures_async_stream::try_stream;
use risingwave_common::util::addr::HostAddr;
use risingwave_pb::connector_service::GetEventStreamResponse;

use crate::parser::ParserConfig;
use crate::source::base::SourceMessage;
use crate::source::cdc::{CdcProperties, CdcSourceType, CdcSourceTypeTrait, DebeziumCdcSplit};
use crate::source::common::{into_chunk_stream, CommonSplitReader};
use crate::source::{
    BoxSourceWithStateStream, Column, SourceContextRef, SplitId, SplitImpl, SplitMetaData,
    SplitReader,
};

pub struct CdcSplitReader<T: CdcSourceTypeTrait> {
    source_id: u64,
    start_offset: Option<String>,
    // host address of worker node for a Citus cluster
    server_addr: Option<String>,
    conn_props: CdcProperties<T>,

    split_id: SplitId,
    // whether the full snapshot phase is done
    snapshot_done: bool,
    parser_config: ParserConfig,
    source_ctx: SourceContextRef,
}

#[async_trait]
impl<T: CdcSourceTypeTrait> SplitReader for CdcSplitReader<T>
where
    DebeziumCdcSplit<T>: TryFrom<SplitImpl, Error = anyhow::Error>,
{
    type Properties = CdcProperties<T>;

    #[allow(clippy::unused_async)]
    async fn new(
        conn_props: CdcProperties<T>,
        splits: Vec<SplitImpl>,
        parser_config: ParserConfig,
        source_ctx: SourceContextRef,
        _columns: Option<Vec<Column>>,
    ) -> Result<Self> {
        assert_eq!(splits.len(), 1);
        let split = DebeziumCdcSplit::<T>::try_from(splits.into_iter().next().unwrap())?;
        let split_id = split.id();
        match T::source_type() {
            CdcSourceType::Mysql | CdcSourceType::Postgres => Ok(Self {
                source_id: split.split_id() as u64,
                start_offset: split.start_offset().clone(),
                server_addr: None,
                conn_props,
                split_id,
                snapshot_done: split.snapshot_done(),
                parser_config,
                source_ctx,
            }),
            CdcSourceType::Citus => Ok(Self {
                source_id: split.split_id() as u64,
                start_offset: split.start_offset().clone(),
                server_addr: split.server_addr().clone(),
                conn_props,
                split_id,
                snapshot_done: split.snapshot_done(),
                parser_config,
                source_ctx,
            }),
        }
    }

    fn into_stream(self) -> BoxSourceWithStateStream {
        let parser_config = self.parser_config.clone();
        let source_context = self.source_ctx.clone();
        into_chunk_stream(self, parser_config, source_context)
    }
}

impl<T: CdcSourceTypeTrait> CommonSplitReader for CdcSplitReader<T>
where
    Self: SplitReader,
{
    #[try_stream(ok = Vec<SourceMessage>, error = anyhow::Error)]
    async fn into_data_stream(self) {
        let cdc_client = self.source_ctx.connector_client.clone().ok_or_else(|| {
            anyhow!("connector node endpoint not specified or unable to connect to connector node")
        })?;

        // rewrite the hostname and port for the split
        let mut properties = self.conn_props.props.clone();

        // For citus, we need to rewrite the table.name to capture sharding tables
        if self.server_addr.is_some() {
            let addr = self.server_addr.unwrap();
            let host_addr = HostAddr::from_str(&addr)
                .map_err(|err| anyhow!("invalid server address for cdc split. {}", err))?;
            properties.insert("hostname".to_string(), host_addr.host);
            properties.insert("port".to_string(), host_addr.port.to_string());
            // rewrite table name with suffix to capture all shards in the split
            let mut table_name = properties
                .remove("table.name")
                .ok_or_else(|| anyhow!("missing field 'table.name'"))?;
            table_name.push_str("_[0-9]+");
            properties.insert("table.name".into(), table_name);
        }

        let cdc_stream = cdc_client
            .start_source_stream(
                self.source_id,
                self.conn_props.get_source_type_pb(),
                self.start_offset,
                properties,
                self.snapshot_done,
            )
            .await
            .inspect_err(|err| tracing::error!("connector node start stream error: {}", err))?;
        pin_mut!(cdc_stream);
        #[for_await]
        for event_res in cdc_stream {
            match event_res {
                Ok(GetEventStreamResponse { events, .. }) => {
                    if events.is_empty() {
                        continue;
                    }
                    let mut msgs = Vec::with_capacity(events.len());
                    for event in events {
                        msgs.push(SourceMessage::from(event));
                    }
                    yield msgs;
                }
                Err(e) => {
                    return Err(anyhow!(
                        "Cdc service error: code {}, msg {}",
                        e.code(),
                        e.message()
                    ))
                }
            }
        }
    }
}
