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

#[macro_export]
macro_rules! impl_split_enumerator {
    ($({ $variant_name:ident, $split_enumerator_name:ident} ),*) => {
        impl SplitEnumeratorImpl {

             pub async fn create(properties: ConnectorProperties, context: SourceEnumeratorContextRef) -> Result<Self> {
                match properties {
                    $( ConnectorProperties::$variant_name(props) => $split_enumerator_name::new(*props, context).await.map(Self::$variant_name), )*
                    other => Err(anyhow!(
                        "split enumerator type for config {:?} is not supported",
                        other
                    )),
                }
             }

             pub async fn list_splits(&mut self) -> Result<Vec<SplitImpl>> {
                match self {
                    $( Self::$variant_name(inner) => inner
                        .list_splits()
                        .await
                        .map(|ss| {
                            ss.into_iter()
                                .map(SplitImpl::$variant_name)
                                .collect_vec()
                        })
                        .map_err(|e| risingwave_common::error::ErrorCode::ConnectorError(e.into()).into()),
                    )*
                }
             }
        }
    }
}

#[macro_export]
macro_rules! impl_split {
    ($({ $variant_name:ident, $connector_name:ident, $split:ty} ),*) => {
        impl From<&SplitImpl> for ConnectorSplit {
            fn from(split: &SplitImpl) -> Self {
                match split {
                    $( SplitImpl::$variant_name(inner) => ConnectorSplit { split_type: String::from($connector_name), encoded_split: inner.encode_to_bytes().to_vec() }, )*
                }
            }
        }
        $(
            impl TryFrom<SplitImpl> for $split {
                type Error = anyhow::Error;

                fn try_from(split: SplitImpl) -> std::result::Result<Self, Self::Error> {
                    match split {
                        SplitImpl::$variant_name(inner) => Ok(inner),
                        other => Err(anyhow::anyhow!("expect {} but get {:?}", stringify!($split), other))
                    }
                }
            }

            impl From<$split> for SplitImpl {
                fn from(split: $split) -> SplitImpl {
                    SplitImpl::$variant_name(split)
                }
            }

        )*

        impl TryFrom<&ConnectorSplit> for SplitImpl {
            type Error = anyhow::Error;

            fn try_from(split: &ConnectorSplit) -> std::result::Result<Self, Self::Error> {
                match split.split_type.to_lowercase().as_str() {
                    $( $connector_name => <$split>::restore_from_bytes(split.encoded_split.as_ref()).map(SplitImpl::$variant_name), )*
                        other => {
                    Err(anyhow!("connector '{}' is not supported", other))
                    }
                }
            }
        }

        impl SplitMetaData for SplitImpl {
            fn id(&self) -> SplitId {
                match self {
                    $( Self::$variant_name(inner) => inner.id(), )*
                }
            }

            fn encode_to_json(&self) -> JsonbVal {
                use serde_json::json;
                let inner = self.encode_to_json_inner().take();
                json!({ SPLIT_TYPE_FIELD: self.get_type(), SPLIT_INFO_FIELD: inner}).into()
            }

            fn restore_from_json(value: JsonbVal) -> Result<Self> {
                let mut value = value.take();
                let json_obj = value.as_object_mut().unwrap();
                let split_type = json_obj.remove(SPLIT_TYPE_FIELD).unwrap().as_str().unwrap().to_string();
                let inner_value = json_obj.remove(SPLIT_INFO_FIELD).unwrap();
                Self::restore_from_json_inner(&split_type, inner_value.into())
            }
        }

        impl SplitImpl {
            pub fn get_type(&self) -> String {
                match self {
                    $( Self::$variant_name(_) => $connector_name, )*
                }
                    .to_string()
            }

            pub fn update_in_place(&mut self, start_offset: String) -> anyhow::Result<()> {
                match self {
                    $( Self::$variant_name(inner) => inner.update_with_offset(start_offset)?, )*
                }
                Ok(())
            }

            pub fn encode_to_json_inner(&self) -> JsonbVal {
                match self {
                    $( Self::$variant_name(inner) => inner.encode_to_json(), )*
                }
            }

            fn restore_from_json_inner(split_type: &str, value: JsonbVal) -> Result<Self> {
                match split_type.to_lowercase().as_str() {
                    $( $connector_name => <$split>::restore_from_json(value).map(SplitImpl::$variant_name), )*
                        other => {
                    Err(anyhow!("connector '{}' is not supported", other))
                    }
                }
            }
        }
    }
}

#[macro_export]
macro_rules! impl_split_reader {
    ($({ $variant_name:ident, $split_reader_name:ident} ),*) => {
        impl SplitReaderImpl {
            pub fn into_stream(self) -> BoxSourceWithStateStream {
                match self {
                    $( Self::$variant_name(inner) => inner.into_stream(), )*                 }
            }

            pub async fn create(
                config: ConnectorProperties,
                state: ConnectorState,
                parser_config: ParserConfig,
                source_ctx: SourceContextRef,
                columns: Option<Vec<Column>>,
            ) -> Result<Self> {
                if state.is_none() {
                    return Ok(Self::Dummy(Box::new(DummySplitReader {})));
                }
                let splits = state.unwrap();
                let connector = match config {
                     $( ConnectorProperties::$variant_name(props) => Self::$variant_name(Box::new($split_reader_name::new(*props, splits, parser_config, source_ctx, columns).await?)), )*
                };

                Ok(connector)
            }
        }
    }
}

#[macro_export]
macro_rules! impl_connector_properties {
    ($({ $variant_name:ident, $connector_name:ident } ),*) => {
        impl ConnectorProperties {
            pub fn extract(mut props: HashMap<String, String>) -> Result<Self> {
                const UPSTREAM_SOURCE_KEY: &str = "connector";
                let connector = props.remove(UPSTREAM_SOURCE_KEY).ok_or_else(|| anyhow!("Must specify 'connector' in WITH clause"))?;
                use $crate::source::cdc::CDC_CONNECTOR_NAME_SUFFIX;
                if connector.ends_with(CDC_CONNECTOR_NAME_SUFFIX) {
                    ConnectorProperties::new_cdc_properties(&connector, props)
                } else {
                    let json_value = serde_json::to_value(props).map_err(|e| anyhow!(e))?;
                    match connector.to_lowercase().as_str() {
                        $(
                            $connector_name => {
                                serde_json::from_value(json_value).map_err(|e| anyhow!(e.to_string())).map(Self::$variant_name)
                            },
                        )*
                        _ => {
                            Err(anyhow!("connector '{}' is not supported", connector,))
                        }
                    }
                }
            }
        }
    }
}

#[macro_export]
macro_rules! impl_cdc_source_type {
    ($({$source_type:ident, $name:expr }),*) => {
        $(
            paste!{
                #[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
                pub struct $source_type;
                impl CdcSourceTypeTrait for $source_type {
                    const CDC_CONNECTOR_NAME: &'static str = concat!($name, "-cdc");
                    fn source_type() -> CdcSourceType {
                        CdcSourceType::$source_type
                    }
                }

                pub type [< $source_type DebeziumSplitEnumerator >] = DebeziumSplitEnumerator<$source_type>;
            }
        )*

        pub enum CdcSourceType {
            $(
                $source_type,
            )*
        }

        impl From<PbSourceType> for CdcSourceType {
            fn from(value: PbSourceType) -> Self {
                match value {
                    PbSourceType::Unspecified => unreachable!(),
                    $(
                        PbSourceType::$source_type => CdcSourceType::$source_type,
                    )*
                }
            }
        }

        impl From<CdcSourceType> for PbSourceType {
            fn from(this: CdcSourceType) -> PbSourceType {
                match this {
                    $(
                        CdcSourceType::$source_type => PbSourceType::$source_type,
                    )*
                }
            }
        }

        impl ConnectorProperties {
            pub(crate) fn new_cdc_properties(
                connector_name: &str,
                properties: HashMap<String, String>,
            ) -> std::result::Result<Self, anyhow::Error> {
                match connector_name {
                    $(
                        $source_type::CDC_CONNECTOR_NAME => paste! {
                            Ok(Self::[< $source_type Cdc >](Box::new(CdcProperties::<$source_type> {
                                props: properties,
                                ..Default::default()
                            })))
                        },
                    )*
                    _ => Err(anyhow::anyhow!("unexpected cdc connector '{}'", connector_name,)),
                }
            }

            pub fn init_cdc_properties(&mut self, table_schema: PbTableSchema) {
                match self {
                    $(
                         paste! {ConnectorProperties:: [< $source_type Cdc >](c)} => {
                            c.table_schema = table_schema;
                         }
                    )*
                    _ => {}
                }
            }

            pub fn is_cdc_connector(&self) -> bool {
                match self {
                    $(
                         paste! {ConnectorProperties:: [< $source_type Cdc >](_)} => true,
                    )*
                    _ => false,
                }
            }
        }
    }
}
