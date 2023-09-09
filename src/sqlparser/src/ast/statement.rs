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

use core::fmt;
use std::collections::BTreeMap;
use std::fmt::Write;

use itertools::Itertools;
#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

use super::ddl::SourceWatermark;
use super::{EmitMode, Ident, ObjectType, Query, Value};
use crate::ast::{
    display_comma_separated, display_separated, ColumnDef, ObjectName, SqlOption, TableConstraint,
};
use crate::keywords::Keyword;
use crate::parser::{IsOptional, Parser, ParserError, UPSTREAM_SOURCE_KEY};
use crate::tokenizer::Token;

/// Consumes token from the parser into an AST node.
pub trait ParseTo: Sized {
    fn parse_to(parser: &mut Parser) -> Result<Self, ParserError>;
}

macro_rules! impl_parse_to {
    () => {};
    ($field:ident : $field_type:ty, $parser:ident) => {
        let $field = <$field_type>::parse_to($parser)?;
    };
    ($field:ident => [$($arr:tt)+], $parser:ident) => {
        let $field = $parser.parse_keywords(&[$($arr)+]);
    };
    ([$($arr:tt)+], $parser:ident) => {
        $parser.expect_keywords(&[$($arr)+])?;
    };
}

macro_rules! impl_fmt_display {
    () => {};
    ($field:ident, $v:ident, $self:ident) => {{
        let s = format!("{}", $self.$field);
        if !s.is_empty() {
            $v.push(s);
        }
    }};
    ($field:ident => [$($arr:tt)+], $v:ident, $self:ident) => {
        if $self.$field {
            $v.push(format!("{}", AstVec([$($arr)+].to_vec())));
        }
    };
    ([$($arr:tt)+], $v:ident) => {
        $v.push(format!("{}", AstVec([$($arr)+].to_vec())));
    };
}

// sql_grammar!(CreateSourceStatement {
//     if_not_exists => [Keyword::IF, Keyword::NOT, Keyword::EXISTS],
//     source_name: Ident,
//     with_properties: AstOption<WithProperties>,
//     [Keyword::ROW, Keyword::FORMAT],
//     source_schema: SourceSchema,
//     [Keyword::WATERMARK, Keyword::FOR] column [Keyword::AS] <expr>
// });
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct CreateSourceStatement {
    pub if_not_exists: bool,
    pub columns: Vec<ColumnDef>,
    pub constraints: Vec<TableConstraint>,
    pub source_name: ObjectName,
    pub with_properties: WithProperties,
    pub source_schema: CompatibleSourceSchema,
    pub source_watermarks: Vec<SourceWatermark>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum SourceSchema {
    Protobuf(ProtobufSchema),
    // Keyword::PROTOBUF ProtobufSchema
    Json,         // Keyword::JSON
    DebeziumJson, // Keyword::DEBEZIUM_JSON
    DebeziumMongoJson,
    UpsertJson,             // Keyword::UPSERT_JSON
    Avro(AvroSchema),       // Keyword::AVRO
    UpsertAvro(AvroSchema), // Keyword::UpsertAVRO
    Maxwell,                // Keyword::MAXWELL
    CanalJson,              // Keyword::CANAL_JSON
    Csv(CsvInfo),           // Keyword::CSV
    Native,
    DebeziumAvro(DebeziumAvroSchema), // Keyword::DEBEZIUM_AVRO
    Bytes,
}

impl SourceSchema {
    pub fn into_source_schema_v2(self) -> SourceSchemaV2 {
        let (format, row_encode) = match self {
            SourceSchema::Protobuf(_) => (Format::Plain, Encode::Protobuf),
            SourceSchema::Json => (Format::Plain, Encode::Json),
            SourceSchema::DebeziumJson => (Format::Debezium, Encode::Json),
            SourceSchema::DebeziumMongoJson => (Format::DebeziumMongo, Encode::Json),
            SourceSchema::UpsertJson => (Format::Upsert, Encode::Json),
            SourceSchema::Avro(_) => (Format::Plain, Encode::Avro),
            SourceSchema::UpsertAvro(_) => (Format::Upsert, Encode::Avro),
            SourceSchema::Maxwell => (Format::Maxwell, Encode::Json),
            SourceSchema::CanalJson => (Format::Canal, Encode::Json),
            SourceSchema::Csv(_) => (Format::Plain, Encode::Csv),
            SourceSchema::DebeziumAvro(_) => (Format::Debezium, Encode::Avro),
            SourceSchema::Bytes => (Format::Plain, Encode::Bytes),
            SourceSchema::Native => (Format::Native, Encode::Native),
        };

        let row_options = match self {
            SourceSchema::Protobuf(schema) => {
                let mut options = vec![SqlOption {
                    name: ObjectName(vec![Ident {
                        value: "message".into(),
                        quote_style: None,
                    }]),
                    value: Value::SingleQuotedString(schema.message_name.0),
                }];
                if schema.use_schema_registry {
                    options.push(SqlOption {
                        name: ObjectName(vec![Ident {
                            value: "schema.registry".into(),
                            quote_style: None,
                        }]),
                        value: Value::SingleQuotedString(schema.row_schema_location.0),
                    });
                } else {
                    options.push(SqlOption {
                        name: ObjectName(vec![Ident {
                            value: "schema.location".into(),
                            quote_style: None,
                        }]),
                        value: Value::SingleQuotedString(schema.row_schema_location.0),
                    })
                }
                options
            }
            SourceSchema::Avro(schema) | SourceSchema::UpsertAvro(schema) => {
                if schema.use_schema_registry {
                    vec![SqlOption {
                        name: ObjectName(vec![Ident {
                            value: "schema.registry".into(),
                            quote_style: None,
                        }]),
                        value: Value::SingleQuotedString(schema.row_schema_location.0),
                    }]
                } else {
                    vec![SqlOption {
                        name: ObjectName(vec![Ident {
                            value: "schema.location".into(),
                            quote_style: None,
                        }]),
                        value: Value::SingleQuotedString(schema.row_schema_location.0),
                    }]
                }
            }
            SourceSchema::DebeziumAvro(schema) => {
                vec![SqlOption {
                    name: ObjectName(vec![Ident {
                        value: "schema.registry".into(),
                        quote_style: None,
                    }]),
                    value: Value::SingleQuotedString(schema.row_schema_location.0),
                }]
            }
            SourceSchema::Csv(schema) => {
                vec![
                    SqlOption {
                        name: ObjectName(vec![Ident {
                            value: "delimiter".into(),
                            quote_style: None,
                        }]),
                        value: Value::SingleQuotedString(
                            String::from_utf8_lossy(&[schema.delimiter]).into(),
                        ),
                    },
                    SqlOption {
                        name: ObjectName(vec![Ident {
                            value: "without_header".into(),
                            quote_style: None,
                        }]),
                        value: Value::SingleQuotedString(if schema.has_header {
                            "false".into()
                        } else {
                            "true".into()
                        }),
                    },
                ]
            }
            _ => vec![],
        };

        SourceSchemaV2 {
            format,
            row_encode,
            row_options,
        }
    }
}

impl fmt::Display for SourceSchema {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SourceSchema::Protobuf(protobuf_schema) => write!(f, "PROTOBUF {}", protobuf_schema),
            SourceSchema::Json => write!(f, "JSON"),
            SourceSchema::UpsertJson => write!(f, "UPSERT_JSON"),
            SourceSchema::Maxwell => write!(f, "MAXWELL"),
            SourceSchema::DebeziumJson => write!(f, "DEBEZIUM_JSON"),
            SourceSchema::DebeziumMongoJson => write!(f, "DEBEZIUM_MONGO_JSON"),
            SourceSchema::Avro(avro_schema) => write!(f, "AVRO {}", avro_schema),
            SourceSchema::UpsertAvro(avro_schema) => write!(f, "UPSERT_AVRO {}", avro_schema),
            SourceSchema::CanalJson => write!(f, "CANAL_JSON"),
            SourceSchema::Csv(csv_info) => write!(f, "CSV {}", csv_info),
            SourceSchema::Native => write!(f, "NATIVE"),
            SourceSchema::DebeziumAvro(avro_schema) => write!(f, "DEBEZIUM_AVRO {}", avro_schema),
            SourceSchema::Bytes => write!(f, "BYTES"),
        }
    }
}

/// will be deprecated and be replaced by Format and Encode
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum RowFormat {
    Protobuf,          // Keyword::PROTOBUF
    Json,              // Keyword::JSON
    DebeziumJson,      // Keyword::DEBEZIUM_JSON
    DebeziumMongoJson, // Keyword::DEBEZIUM_MONGO_JSON
    UpsertJson,        // Keyword::UPSERT_JSON
    Avro,              // Keyword::AVRO
    UpsertAvro,        // Keyword::UpsertAVRO
    Maxwell,           // Keyword::MAXWELL
    CanalJson,         // Keyword::CANAL_JSON
    Csv,               // Keyword::CSV
    DebeziumAvro,      // Keyword::DEBEZIUM_AVRO
    Bytes,             // Keyword::BYTES
    Native,
}

impl RowFormat {
    pub fn from_keyword(s: &str) -> Result<Self, ParserError> {
        Ok(match s {
            "JSON" => RowFormat::Json,
            "UPSERT_JSON" => RowFormat::UpsertJson,
            "PROTOBUF" => RowFormat::Protobuf,
            "DEBEZIUM_JSON" => RowFormat::DebeziumJson,
            "DEBEZIUM_MONGO_JSON" => RowFormat::DebeziumMongoJson,
            "AVRO" => RowFormat::Avro,
            "UPSERT_AVRO" => RowFormat::UpsertAvro,
            "MAXWELL" => RowFormat::Maxwell,
            "CANAL_JSON" => RowFormat::CanalJson,
            "CSV" => RowFormat::Csv,
            "DEBEZIUM_AVRO" => RowFormat::DebeziumAvro,
            "BYTES" => RowFormat::Bytes,
             _ => return Err(ParserError::ParserError(
                "expected JSON | UPSERT_JSON | PROTOBUF | DEBEZIUM_JSON | DEBEZIUM_AVRO | AVRO | UPSERT_AVRO | MAXWELL | CANAL_JSON | BYTES after ROW FORMAT".to_string(),
            ))
        })
    }

    /// a compatibility layer, return (format, row_encode)
    pub fn to_format_v2(&self) -> (Format, Encode) {
        let format = match self {
            RowFormat::Protobuf => Format::Plain,
            RowFormat::Json => Format::Plain,
            RowFormat::DebeziumJson => Format::Debezium,
            RowFormat::DebeziumMongoJson => Format::DebeziumMongo,
            RowFormat::UpsertJson => Format::Upsert,
            RowFormat::Avro => Format::Plain,
            RowFormat::UpsertAvro => Format::Upsert,
            RowFormat::Maxwell => Format::Maxwell,
            RowFormat::CanalJson => Format::Canal,
            RowFormat::Csv => Format::Plain,
            RowFormat::DebeziumAvro => Format::Debezium,
            RowFormat::Bytes => Format::Plain,
            RowFormat::Native => Format::Native,
        };

        let encode = match self {
            RowFormat::Protobuf => Encode::Protobuf,
            RowFormat::Json => Encode::Json,
            RowFormat::DebeziumJson => Encode::Json,
            RowFormat::DebeziumMongoJson => Encode::Json,
            RowFormat::UpsertJson => Encode::Json,
            RowFormat::Avro => Encode::Avro,
            RowFormat::UpsertAvro => Encode::Avro,
            RowFormat::Maxwell => Encode::Json,
            RowFormat::CanalJson => Encode::Json,
            RowFormat::Csv => Encode::Csv,
            RowFormat::DebeziumAvro => Encode::Avro,
            RowFormat::Bytes => Encode::Bytes,
            RowFormat::Native => Encode::Native,
        };
        (format, encode)
    }

    /// a compatibility layer
    pub fn from_format_v2(format: &Format, encode: &Encode) -> Result<Self, ParserError> {
        Ok(match (format, encode) {
            (Format::Native, Encode::Native) => RowFormat::Native,
            (Format::Native, _) => unreachable!(),
            (_, Encode::Native) => unreachable!(),
            (Format::Debezium, Encode::Avro) => RowFormat::DebeziumAvro,
            (Format::Debezium, Encode::Json) => RowFormat::DebeziumJson,
            (Format::Debezium, _) => {
                return Err(ParserError::ParserError(
                    "The DEBEZIUM format only support AVRO and JSON Encoding".to_string(),
                ))
            }
            (Format::DebeziumMongo, Encode::Json) => RowFormat::DebeziumMongoJson,
            (Format::DebeziumMongo, _) => {
                return Err(ParserError::ParserError(
                    "The DEBEZIUM_MONGO format only support JSON Encoding".to_string(),
                ))
            }
            (Format::Maxwell, Encode::Json) => RowFormat::Maxwell,
            (Format::Maxwell, _) => {
                return Err(ParserError::ParserError(
                    "The MAXWELL format only support JSON Encoding".to_string(),
                ))
            }
            (Format::Canal, Encode::Json) => RowFormat::CanalJson,
            (Format::Canal, _) => {
                return Err(ParserError::ParserError(
                    "The CANAL format only support JSON Encoding".to_string(),
                ))
            }
            (Format::Upsert, Encode::Avro) => RowFormat::UpsertAvro,
            (Format::Upsert, Encode::Json) => RowFormat::UpsertJson,
            (Format::Upsert, _) => {
                return Err(ParserError::ParserError(
                    "The UPSERT format only support AVRO and JSON Encoding".to_string(),
                ))
            }
            (Format::Plain, Encode::Avro) => RowFormat::Avro,
            (Format::Plain, Encode::Csv) => RowFormat::Csv,
            (Format::Plain, Encode::Protobuf) => RowFormat::Protobuf,
            (Format::Plain, Encode::Json) => RowFormat::Json,
            (Format::Plain, Encode::Bytes) => RowFormat::Bytes,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum Format {
    Native,
    Debezium,      // Keyword::DEBEZIUM
    DebeziumMongo, // Keyword::DEBEZIUM_MONGO
    Maxwell,       // Keyword::MAXWELL
    Canal,         // Keyword::CANAL
    Upsert,        // Keyword::UPSERT
    Plain,         // Keyword::PLAIN
}

// TODO: unify with `from_keyword`
impl fmt::Display for Format {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}",
            match self {
                Format::Native => "NATIVE",
                Format::Debezium => "DEBEZIUM",
                Format::DebeziumMongo => "DEBEZIUM_MONGO",
                Format::Maxwell => "MAXWELL",
                Format::Canal => "CANAL",
                Format::Upsert => "UPSERT",
                Format::Plain => "PLAIN",
            }
        )
    }
}

impl Format {
    pub fn from_keyword(s: &str) -> Result<Self, ParserError> {
        Ok(match s {
            "DEBEZIUM" => Format::Debezium,
            "DEBEZIUM_MONGO" => Format::DebeziumMongo,
            "MAXWELL" => Format::Maxwell,
            "CANAL" => Format::Canal,
            "PLAIN" => Format::Plain,
            "UPSERT" => Format::Upsert,
            _ => {
                return Err(ParserError::ParserError(
                    "expected CANAL | PROTOBUF | DEBEZIUM | MAXWELL | Plain after FORMAT"
                        .to_string(),
                ))
            }
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum Encode {
    Avro,     // Keyword::Avro
    Csv,      // Keyword::CSV
    Protobuf, // Keyword::PROTOBUF
    Json,     // Keyword::JSON
    Bytes,    // Keyword::BYTES
    Native,
}

// TODO: unify with `from_keyword`
impl fmt::Display for Encode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}",
            match self {
                Encode::Avro => "AVRO",
                Encode::Csv => "CSV",
                Encode::Protobuf => "PROTOBUF",
                Encode::Json => "JSON",
                Encode::Bytes => "BYTES",
                Encode::Native => "NATIVE",
            }
        )
    }
}

impl Encode {
    pub fn from_keyword(s: &str) -> Result<Self, ParserError> {
        Ok(match s {
            "AVRO" => Encode::Avro,
            "BYTES" => Encode::Bytes,
            "CSV" => Encode::Csv,
            "PROTOBUF" => Encode::Protobuf,
            "JSON" => Encode::Json,
            _ => {
                return Err(ParserError::ParserError(
                    "expected AVRO | BYTES | CSV | PROTOBUF | JSON after Encode".to_string(),
                ))
            }
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct SourceSchemaV2 {
    pub format: Format,
    pub row_encode: Encode,
    pub row_options: Vec<SqlOption>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum CompatibleSourceSchema {
    RowFormat(SourceSchema),
    V2(SourceSchemaV2),
}

impl fmt::Display for CompatibleSourceSchema {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CompatibleSourceSchema::RowFormat(inner) => {
                write!(f, "{}", inner)
            }
            CompatibleSourceSchema::V2(inner) => {
                write!(f, "{}", inner)
            }
        }
    }
}

impl CompatibleSourceSchema {
    pub fn into_source_schema(
        self,
    ) -> Result<(SourceSchema, Vec<SqlOption>, Option<String>), ParserError> {
        match self {
            CompatibleSourceSchema::RowFormat(inner) => Ok((
                inner,
                vec![],
                Some("RisingWave will stop supporting the syntax \"ROW FORMAT\" in future versions, which will be changed to \"FORMAT ... ENCODE ...\" syntax.".to_string()),
            )),
            CompatibleSourceSchema::V2(inner) => {
                inner.into_source_schema().map(|(s, ops)| (s, ops, None))
            }
        }
    }

    pub fn into_source_schema_v2(self) -> SourceSchemaV2 {
        match self {
            CompatibleSourceSchema::RowFormat(inner) => inner.into_source_schema_v2(),
            CompatibleSourceSchema::V2(inner) => inner,
        }
    }
}

impl From<SourceSchemaV2> for CompatibleSourceSchema {
    fn from(value: SourceSchemaV2) -> Self {
        Self::V2(value)
    }
}

pub fn parse_source_shcema(p: &mut Parser) -> Result<CompatibleSourceSchema, ParserError> {
    if p.peek_nth_any_of_keywords(0, &[Keyword::FORMAT]) {
        p.expect_keyword(Keyword::FORMAT)?;
        let id = p.parse_identifier()?;
        let s = id.value.to_ascii_uppercase();
        let format = Format::from_keyword(&s)?;
        p.expect_keyword(Keyword::ENCODE)?;
        let id = p.parse_identifier()?;
        let s = id.value.to_ascii_uppercase();
        let row_encode = Encode::from_keyword(&s)?;
        let row_options = p.parse_options()?;

        Ok(CompatibleSourceSchema::V2(SourceSchemaV2 {
            format,
            row_encode,
            row_options,
        }))
    } else if p.peek_nth_any_of_keywords(0, &[Keyword::ROW])
        && p.peek_nth_any_of_keywords(1, &[Keyword::FORMAT])
    {
        p.expect_keyword(Keyword::ROW)?;
        p.expect_keyword(Keyword::FORMAT)?;
        let id = p.parse_identifier()?;
        let value = id.value.to_ascii_uppercase();
        let schema = match &value[..] {
            "JSON" => SourceSchema::Json,
            "UPSERT_JSON" => SourceSchema::UpsertJson,
            "PROTOBUF" => {
                impl_parse_to!(protobuf_schema: ProtobufSchema, p);
                SourceSchema::Protobuf(protobuf_schema)
            }
            "DEBEZIUM_JSON" => SourceSchema::DebeziumJson,
            "DEBEZIUM_MONGO_JSON" => SourceSchema::DebeziumMongoJson,
            "AVRO" => {
                impl_parse_to!(avro_schema: AvroSchema, p);
                SourceSchema::Avro(avro_schema)
            }
            "UPSERT_AVRO" => {
                impl_parse_to!(avro_schema: AvroSchema, p);
                SourceSchema::UpsertAvro(avro_schema)
            }
            "MAXWELL" => SourceSchema::Maxwell,
            "CANAL_JSON" => SourceSchema::CanalJson,
            "CSV" => {
                impl_parse_to!(csv_info: CsvInfo, p);
                SourceSchema::Csv(csv_info)
            }
            "DEBEZIUM_AVRO" => {
                impl_parse_to!(avro_schema: DebeziumAvroSchema, p);
                SourceSchema::DebeziumAvro(avro_schema)
            }
            "BYTES" => {
                SourceSchema::Bytes
            }
            _ => return Err(ParserError::ParserError(
                "expected JSON | UPSERT_JSON | PROTOBUF | DEBEZIUM_JSON | DEBEZIUM_AVRO | AVRO | UPSERT_AVRO | MAXWELL | CANAL_JSON | BYTES after ROW FORMAT".to_string(),
            ))

        }    ;
        Ok(CompatibleSourceSchema::RowFormat(schema))
    } else {
        Err(ParserError::ParserError(
            "expect description of the format".to_string(),
        ))
    }
}

impl SourceSchemaV2 {
    pub fn gen_options(&self) -> Result<BTreeMap<String, String>, ParserError> {
        self.row_options
            .iter()
            .cloned()
            .map(|x| match x.value {
                Value::CstyleEscapedString(s) => Ok((x.name.real_value(), s.value)),
                Value::SingleQuotedString(s) => Ok((x.name.real_value(), s)),
                Value::Number(n) => Ok((x.name.real_value(), n)),
                Value::Boolean(b) => Ok((x.name.real_value(), b.to_string())),
                _ => Err(ParserError::ParserError(
                    "`row format options` only support single quoted string value and C style escaped string".to_owned(),
                )),
            })
            .try_collect()
    }

    /// just a temporal compatibility layer will be removed soon(so the implementation is a little
    /// dirty)
    #[allow(deprecated)]
    pub fn into_source_schema(self) -> Result<(SourceSchema, Vec<SqlOption>), ParserError> {
        let options: BTreeMap<String, String> = self
            .row_options
            .iter()
            .cloned()
            .map(|x| match x.value {
                Value::SingleQuotedString(s) => Ok((x.name.real_value(), s)),
                Value::Number(n) => Ok((x.name.real_value(), n)),
                Value::Boolean(b) => Ok((x.name.real_value(), b.to_string())),
                _ => Err(ParserError::ParserError(
                    "`row format options` only support single quoted string value".to_owned(),
                )),
            })
            .try_collect()?;

        let try_consume_string_from_options =
            |row_options: &BTreeMap<String, String>, key: &str| -> Option<AstString> {
                row_options.get(key).cloned().map(AstString)
            };
        let consume_string_from_options =
            |row_options: &BTreeMap<String, String>, key: &str| -> Result<AstString, ParserError> {
                try_consume_string_from_options(row_options, key).ok_or_else(|| {
                    ParserError::ParserError(format!("missing field {} in row format options", key))
                })
            };
        let get_schema_location =
            |row_options: &BTreeMap<String, String>| -> Result<(AstString, bool), ParserError> {
                let schema_location =
                    try_consume_string_from_options(row_options, "schema.location");
                let schema_registry =
                    try_consume_string_from_options(row_options, "schema.registry");
                match (schema_location, schema_registry) {
                    (None, None) => Err(ParserError::ParserError(
                        "missing either a schema location or a schema registry".to_string(),
                    )),
                    (None, Some(schema_registry)) => Ok((schema_registry, true)),
                    (Some(schema_location), None) => Ok((schema_location, false)),
                    (Some(_), Some(_)) => Err(ParserError::ParserError(
                        "only need either the schema location or the schema registry".to_string(),
                    )),
                }
            };
        let row_format = RowFormat::from_format_v2(&self.format, &self.row_encode)?;
        Ok((
            match row_format {
                RowFormat::Protobuf => {
                    let (row_schema_location, use_schema_registry) = get_schema_location(&options)?;
                    SourceSchema::Protobuf(ProtobufSchema {
                        message_name: consume_string_from_options(&options, "message")?,
                        row_schema_location,
                        use_schema_registry,
                    })
                }
                RowFormat::Json => SourceSchema::Json,
                RowFormat::DebeziumJson => SourceSchema::DebeziumJson,
                RowFormat::DebeziumMongoJson => SourceSchema::DebeziumMongoJson,
                RowFormat::UpsertJson => SourceSchema::UpsertJson,
                RowFormat::Avro => {
                    let (row_schema_location, use_schema_registry) = get_schema_location(&options)?;
                    SourceSchema::Avro(AvroSchema {
                        row_schema_location,
                        use_schema_registry,
                    })
                }
                RowFormat::UpsertAvro => {
                    let (row_schema_location, use_schema_registry) = get_schema_location(&options)?;
                    SourceSchema::UpsertAvro(AvroSchema {
                        row_schema_location,
                        use_schema_registry,
                    })
                }
                RowFormat::Maxwell => SourceSchema::Maxwell,
                RowFormat::CanalJson => SourceSchema::CanalJson,
                RowFormat::Csv => {
                    let chars = consume_string_from_options(&options, "delimiter")?.0;
                    let delimiter = get_delimiter(chars.as_str())?;
                    let has_header = try_consume_string_from_options(&options, "without_header")
                        .map(|s| s.0 == "false")
                        .unwrap_or(true);
                    SourceSchema::Csv(CsvInfo {
                        delimiter,
                        has_header,
                    })
                }
                RowFormat::DebeziumAvro => {
                    let (row_schema_location, use_schema_registry) = get_schema_location(&options)?;
                    if !use_schema_registry {
                        return Err(ParserError::ParserError(
                            "schema location for DEBEZIUM_AVRO row format is not supported"
                                .to_string(),
                        ));
                    }
                    SourceSchema::DebeziumAvro(DebeziumAvroSchema {
                        row_schema_location,
                    })
                }
                RowFormat::Bytes => SourceSchema::Bytes,
                RowFormat::Native => SourceSchema::Native,
            },
            self.row_options,
        ))
    }

    pub fn row_options(&self) -> &[SqlOption] {
        self.row_options.as_ref()
    }
}

impl fmt::Display for SourceSchemaV2 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "FORMAT {} ENCODE {}", self.format, self.row_encode)?;

        if !self.row_options().is_empty() {
            write!(f, " ({})", display_comma_separated(self.row_options()))
        } else {
            Ok(())
        }
    }
}

// sql_grammar!(ProtobufSchema {
//     [Keyword::MESSAGE],
//     message_name: AstString,
//     [Keyword::ROW, Keyword::SCHEMA, Keyword::LOCATION],
//     row_schema_location: AstString,
// });
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct ProtobufSchema {
    pub message_name: AstString,
    pub row_schema_location: AstString,
    pub use_schema_registry: bool,
}

impl ParseTo for ProtobufSchema {
    fn parse_to(p: &mut Parser) -> Result<Self, ParserError> {
        impl_parse_to!([Keyword::MESSAGE], p);
        impl_parse_to!(message_name: AstString, p);
        impl_parse_to!([Keyword::ROW, Keyword::SCHEMA, Keyword::LOCATION], p);
        impl_parse_to!(use_schema_registry => [Keyword::CONFLUENT, Keyword::SCHEMA, Keyword::REGISTRY], p);
        impl_parse_to!(row_schema_location: AstString, p);
        Ok(Self {
            message_name,
            row_schema_location,
            use_schema_registry,
        })
    }
}

impl fmt::Display for ProtobufSchema {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut v: Vec<String> = vec![];
        impl_fmt_display!([Keyword::MESSAGE], v);
        impl_fmt_display!(message_name, v, self);
        impl_fmt_display!([Keyword::ROW, Keyword::SCHEMA, Keyword::LOCATION], v);
        impl_fmt_display!(use_schema_registry => [Keyword::CONFLUENT, Keyword::SCHEMA, Keyword::REGISTRY], v, self);
        impl_fmt_display!(row_schema_location, v, self);
        v.iter().join(" ").fmt(f)
    }
}

// sql_grammar!(AvroSchema {
//     [Keyword::ROW, Keyword::SCHEMA, Keyword::LOCATION, [Keyword::CONFLUENT, Keyword::SCHEMA,
// Keyword::REGISTRY]],     row_schema_location: AstString,
// });
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct AvroSchema {
    pub row_schema_location: AstString,
    pub use_schema_registry: bool,
}
impl ParseTo for AvroSchema {
    fn parse_to(p: &mut Parser) -> Result<Self, ParserError> {
        impl_parse_to!([Keyword::ROW, Keyword::SCHEMA, Keyword::LOCATION], p);
        impl_parse_to!(use_schema_registry => [Keyword::CONFLUENT, Keyword::SCHEMA, Keyword::REGISTRY], p);
        impl_parse_to!(row_schema_location: AstString, p);
        Ok(Self {
            row_schema_location,
            use_schema_registry,
        })
    }
}

impl fmt::Display for AvroSchema {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut v: Vec<String> = vec![];
        impl_fmt_display!([Keyword::ROW, Keyword::SCHEMA, Keyword::LOCATION], v);
        impl_fmt_display!(use_schema_registry => [Keyword::CONFLUENT, Keyword::SCHEMA, Keyword::REGISTRY], v, self);
        impl_fmt_display!(row_schema_location, v, self);
        v.iter().join(" ").fmt(f)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct DebeziumAvroSchema {
    pub row_schema_location: AstString,
}

impl fmt::Display for DebeziumAvroSchema {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut v: Vec<String> = vec![];
        impl_fmt_display!(
            [
                Keyword::ROW,
                Keyword::SCHEMA,
                Keyword::LOCATION,
                Keyword::CONFLUENT,
                Keyword::SCHEMA,
                Keyword::REGISTRY
            ],
            v
        );
        impl_fmt_display!(row_schema_location, v, self);
        v.iter().join(" ").fmt(f)
    }
}

impl ParseTo for DebeziumAvroSchema {
    fn parse_to(p: &mut Parser) -> Result<Self, ParserError> {
        impl_parse_to!(
            [
                Keyword::ROW,
                Keyword::SCHEMA,
                Keyword::LOCATION,
                Keyword::CONFLUENT,
                Keyword::SCHEMA,
                Keyword::REGISTRY
            ],
            p
        );
        impl_parse_to!(row_schema_location: AstString, p);
        Ok(Self {
            row_schema_location,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct CsvInfo {
    pub delimiter: u8,
    pub has_header: bool,
}

pub fn get_delimiter(chars: &str) -> Result<u8, ParserError> {
    match chars {
        "," => Ok(b','),   // comma
        "\t" => Ok(b'\t'), // tab
        other => Err(ParserError::ParserError(format!(
            "The delimiter should be one of ',', E'\\t', but got {:?}",
            other
        ))),
    }
}

impl ParseTo for CsvInfo {
    fn parse_to(p: &mut Parser) -> Result<Self, ParserError> {
        impl_parse_to!(without_header => [Keyword::WITHOUT, Keyword::HEADER], p);
        impl_parse_to!([Keyword::DELIMITED, Keyword::BY], p);
        impl_parse_to!(delimiter: AstString, p);
        let delimiter = get_delimiter(delimiter.0.as_str())?;
        Ok(Self {
            delimiter,
            has_header: !without_header,
        })
    }
}

impl fmt::Display for CsvInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut v: Vec<String> = vec![];
        if !self.has_header {
            v.push(format!(
                "{}",
                AstVec([Keyword::WITHOUT, Keyword::HEADER].to_vec())
            ));
        }
        impl_fmt_display!(delimiter, v, self);
        v.iter().join(" ").fmt(f)
    }
}

impl ParseTo for CreateSourceStatement {
    fn parse_to(p: &mut Parser) -> Result<Self, ParserError> {
        impl_parse_to!(if_not_exists => [Keyword::IF, Keyword::NOT, Keyword::EXISTS], p);
        impl_parse_to!(source_name: ObjectName, p);

        // parse columns
        let (columns, constraints, source_watermarks) = p.parse_columns_with_watermark()?;

        let with_options = p.parse_with_properties()?;
        let option = with_options
            .iter()
            .find(|&opt| opt.name.real_value() == UPSTREAM_SOURCE_KEY);
        let connector: String = option.map(|opt| opt.value.to_string()).unwrap_or_default();
        // row format for cdc source must be debezium json
        // row format for nexmark source must be native
        // default row format for datagen source is native
        let source_schema = if connector.contains("-cdc") {
            if (p.peek_nth_any_of_keywords(0, &[Keyword::ROW])
                && p.peek_nth_any_of_keywords(1, &[Keyword::FORMAT]))
                || p.peek_nth_any_of_keywords(0, &[Keyword::FORMAT])
            {
                return Err(ParserError::ParserError("Row format for cdc connectors should not be set here because it is limited to debezium json".to_string()));
            }
            SourceSchemaV2 {
                format: Format::Debezium,
                row_encode: Encode::Json,
                row_options: Default::default(),
            }
            .into()
        } else if connector.contains("nexmark") {
            if (p.peek_nth_any_of_keywords(0, &[Keyword::ROW])
                && p.peek_nth_any_of_keywords(1, &[Keyword::FORMAT]))
                || p.peek_nth_any_of_keywords(0, &[Keyword::FORMAT])
            {
                return Err(ParserError::ParserError("Row format for nexmark connectors should not be set here because it is limited to internal native format".to_string()));
            }
            SourceSchemaV2 {
                format: Format::Native,
                row_encode: Encode::Native,
                row_options: Default::default(),
            }
            .into()
        } else if connector.contains("datagen") {
            if (p.peek_nth_any_of_keywords(0, &[Keyword::ROW])
                && p.peek_nth_any_of_keywords(1, &[Keyword::FORMAT]))
                || p.peek_nth_any_of_keywords(0, &[Keyword::FORMAT])
            {
                parse_source_shcema(p)?
            } else {
                SourceSchemaV2 {
                    format: Format::Native,
                    row_encode: Encode::Native,
                    row_options: Default::default(),
                }
                .into()
            }
        } else {
            parse_source_shcema(p)?
        };

        Ok(Self {
            if_not_exists,
            columns,
            constraints,
            source_name,
            with_properties: WithProperties(with_options),
            source_schema,
            source_watermarks,
        })
    }
}

pub(super) fn fmt_create_items(
    columns: &[ColumnDef],
    constraints: &[TableConstraint],
    watermarks: &[SourceWatermark],
) -> std::result::Result<String, fmt::Error> {
    let mut items = String::new();
    let has_items = !columns.is_empty() || !constraints.is_empty() || !watermarks.is_empty();
    has_items.then(|| write!(&mut items, "("));
    write!(&mut items, "{}", display_comma_separated(columns))?;
    if !columns.is_empty() && (!constraints.is_empty() || !watermarks.is_empty()) {
        write!(&mut items, ", ")?;
    }
    write!(&mut items, "{}", display_comma_separated(constraints))?;
    if !columns.is_empty() && !constraints.is_empty() && !watermarks.is_empty() {
        write!(&mut items, ", ")?;
    }
    write!(&mut items, "{}", display_comma_separated(watermarks))?;
    has_items.then(|| write!(&mut items, ")"));
    Ok(items)
}

impl fmt::Display for CreateSourceStatement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut v: Vec<String> = vec![];
        impl_fmt_display!(if_not_exists => [Keyword::IF, Keyword::NOT, Keyword::EXISTS], v, self);
        impl_fmt_display!(source_name, v, self);

        let items = fmt_create_items(&self.columns, &self.constraints, &self.source_watermarks)?;
        if !items.is_empty() {
            v.push(items);
        }

        impl_fmt_display!(with_properties, v, self);
        impl_fmt_display!(source_schema, v, self);
        v.iter().join(" ").fmt(f)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum CreateSink {
    From(ObjectName),
    AsQuery(Box<Query>),
}

impl fmt::Display for CreateSink {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::From(mv) => write!(f, "FROM {}", mv),
            Self::AsQuery(query) => write!(f, "AS {}", query),
        }
    }
}

// sql_grammar!(CreateSinkStatement {
//     if_not_exists => [Keyword::IF, Keyword::NOT, Keyword::EXISTS],
//     sink_name: Ident,
//     [Keyword::FROM],
//     materialized_view: Ident,
//     with_properties: AstOption<WithProperties>,
// });
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct CreateSinkStatement {
    pub if_not_exists: bool,
    pub sink_name: ObjectName,
    pub with_properties: WithProperties,
    pub sink_from: CreateSink,
    pub columns: Vec<Ident>,
    pub emit_mode: Option<EmitMode>,
}

impl ParseTo for CreateSinkStatement {
    fn parse_to(p: &mut Parser) -> Result<Self, ParserError> {
        impl_parse_to!(if_not_exists => [Keyword::IF, Keyword::NOT, Keyword::EXISTS], p);
        impl_parse_to!(sink_name: ObjectName, p);

        let columns = p.parse_parenthesized_column_list(IsOptional::Optional)?;

        let sink_from = if p.parse_keyword(Keyword::FROM) {
            impl_parse_to!(from_name: ObjectName, p);
            CreateSink::From(from_name)
        } else if p.parse_keyword(Keyword::AS) {
            let query = Box::new(p.parse_query()?);
            CreateSink::AsQuery(query)
        } else {
            p.expected("FROM or AS after CREATE SINK sink_name", p.peek_token())?
        };

        let emit_mode = p.parse_emit_mode()?;

        impl_parse_to!(with_properties: WithProperties, p);
        if with_properties.0.is_empty() {
            return Err(ParserError::ParserError(
                "sink properties not provided".to_string(),
            ));
        }

        Ok(Self {
            if_not_exists,
            sink_name,
            with_properties,
            sink_from,
            columns,
            emit_mode,
        })
    }
}

impl fmt::Display for CreateSinkStatement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut v: Vec<String> = vec![];
        impl_fmt_display!(if_not_exists => [Keyword::IF, Keyword::NOT, Keyword::EXISTS], v, self);
        impl_fmt_display!(sink_name, v, self);
        impl_fmt_display!(sink_from, v, self);
        if let Some(ref emit_mode) = self.emit_mode {
            v.push(format!("EMIT {}", emit_mode));
        }
        impl_fmt_display!(with_properties, v, self);
        v.iter().join(" ").fmt(f)
    }
}

// sql_grammar!(CreateConnectionStatement {
//     if_not_exists => [Keyword::IF, Keyword::NOT, Keyword::EXISTS],
//     connection_name: Ident,
//     with_properties: AstOption<WithProperties>,
// });
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct CreateConnectionStatement {
    pub if_not_exists: bool,
    pub connection_name: ObjectName,
    pub with_properties: WithProperties,
}

impl ParseTo for CreateConnectionStatement {
    fn parse_to(p: &mut Parser) -> Result<Self, ParserError> {
        impl_parse_to!(if_not_exists => [Keyword::IF, Keyword::NOT, Keyword::EXISTS], p);
        impl_parse_to!(connection_name: ObjectName, p);
        impl_parse_to!(with_properties: WithProperties, p);
        if with_properties.0.is_empty() {
            return Err(ParserError::ParserError(
                "connection properties not provided".to_string(),
            ));
        }

        Ok(Self {
            if_not_exists,
            connection_name,
            with_properties,
        })
    }
}

impl fmt::Display for CreateConnectionStatement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut v: Vec<String> = vec![];
        impl_fmt_display!(if_not_exists => [Keyword::IF, Keyword::NOT, Keyword::EXISTS], v, self);
        impl_fmt_display!(connection_name, v, self);
        impl_fmt_display!(with_properties, v, self);
        v.iter().join(" ").fmt(f)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct AstVec<T>(pub Vec<T>);

impl<T: fmt::Display> fmt::Display for AstVec<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.iter().join(" ").fmt(f)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct WithProperties(pub Vec<SqlOption>);

impl ParseTo for WithProperties {
    fn parse_to(parser: &mut Parser) -> Result<Self, ParserError> {
        Ok(Self(
            parser.parse_options_with_preceding_keyword(Keyword::WITH)?,
        ))
    }
}

impl fmt::Display for WithProperties {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if !self.0.is_empty() {
            write!(f, "WITH ({})", display_comma_separated(self.0.as_slice()))
        } else {
            Ok(())
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct RowSchemaLocation {
    pub value: AstString,
}

impl ParseTo for RowSchemaLocation {
    fn parse_to(p: &mut Parser) -> Result<Self, ParserError> {
        impl_parse_to!([Keyword::ROW, Keyword::SCHEMA, Keyword::LOCATION], p);
        impl_parse_to!(value: AstString, p);
        Ok(Self { value })
    }
}

impl fmt::Display for RowSchemaLocation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut v = vec![];
        impl_fmt_display!([Keyword::ROW, Keyword::SCHEMA, Keyword::LOCATION], v);
        impl_fmt_display!(value, v, self);
        v.iter().join(" ").fmt(f)
    }
}

/// String literal. The difference with String is that it is displayed with
/// single-quotes.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct AstString(pub String);

impl ParseTo for AstString {
    fn parse_to(parser: &mut Parser) -> Result<Self, ParserError> {
        Ok(Self(parser.parse_literal_string()?))
    }
}

impl fmt::Display for AstString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "'{}'", self.0)
    }
}

/// This trait is used to replace `Option` because `fmt::Display` can not be implemented for
/// `Option<T>`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum AstOption<T> {
    /// No value
    None,
    /// Some value `T`
    Some(T),
}

impl<T: ParseTo> ParseTo for AstOption<T> {
    fn parse_to(parser: &mut Parser) -> Result<Self, ParserError> {
        match T::parse_to(parser) {
            Ok(t) => Ok(AstOption::Some(t)),
            Err(_) => Ok(AstOption::None),
        }
    }
}

impl<T: fmt::Display> fmt::Display for AstOption<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self {
            AstOption::Some(t) => t.fmt(f),
            AstOption::None => Ok(()),
        }
    }
}

impl<T> From<AstOption<T>> for Option<T> {
    fn from(val: AstOption<T>) -> Self {
        match val {
            AstOption::Some(t) => Some(t),
            AstOption::None => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct CreateUserStatement {
    pub user_name: ObjectName,
    pub with_options: UserOptions,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct AlterUserStatement {
    pub user_name: ObjectName,
    pub mode: AlterUserMode,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum AlterUserMode {
    Options(UserOptions),
    Rename(ObjectName),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum UserOption {
    SuperUser,
    NoSuperUser,
    CreateDB,
    NoCreateDB,
    CreateUser,
    NoCreateUser,
    Login,
    NoLogin,
    EncryptedPassword(AstString),
    Password(Option<AstString>),
}

impl fmt::Display for UserOption {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            UserOption::SuperUser => write!(f, "SUPERUSER"),
            UserOption::NoSuperUser => write!(f, "NOSUPERUSER"),
            UserOption::CreateDB => write!(f, "CREATEDB"),
            UserOption::NoCreateDB => write!(f, "NOCREATEDB"),
            UserOption::CreateUser => write!(f, "CREATEUSER"),
            UserOption::NoCreateUser => write!(f, "NOCREATEUSER"),
            UserOption::Login => write!(f, "LOGIN"),
            UserOption::NoLogin => write!(f, "NOLOGIN"),
            UserOption::EncryptedPassword(p) => write!(f, "ENCRYPTED PASSWORD {}", p),
            UserOption::Password(None) => write!(f, "PASSWORD NULL"),
            UserOption::Password(Some(p)) => write!(f, "PASSWORD {}", p),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct UserOptions(pub Vec<UserOption>);

#[derive(Default)]
struct UserOptionsBuilder {
    super_user: Option<UserOption>,
    create_db: Option<UserOption>,
    create_user: Option<UserOption>,
    login: Option<UserOption>,
    password: Option<UserOption>,
}

impl UserOptionsBuilder {
    fn build(self) -> UserOptions {
        let mut options = vec![];
        if let Some(option) = self.super_user {
            options.push(option);
        }
        if let Some(option) = self.create_db {
            options.push(option);
        }
        if let Some(option) = self.create_user {
            options.push(option);
        }
        if let Some(option) = self.login {
            options.push(option);
        }
        if let Some(option) = self.password {
            options.push(option);
        }
        UserOptions(options)
    }
}

impl ParseTo for UserOptions {
    fn parse_to(parser: &mut Parser) -> Result<Self, ParserError> {
        let mut builder = UserOptionsBuilder::default();
        let add_option = |item: &mut Option<UserOption>, user_option| {
            let old_value = item.replace(user_option);
            if old_value.is_some() {
                Err(ParserError::ParserError(
                    "conflicting or redundant options".to_string(),
                ))
            } else {
                Ok(())
            }
        };
        let _ = parser.parse_keyword(Keyword::WITH);
        loop {
            let token = parser.peek_token();
            if token == Token::EOF || token == Token::SemiColon {
                break;
            }

            if let Token::Word(ref w) = token.token {
                parser.next_token();
                let (item_mut_ref, user_option) = match w.keyword {
                    Keyword::SUPERUSER => (&mut builder.super_user, UserOption::SuperUser),
                    Keyword::NOSUPERUSER => (&mut builder.super_user, UserOption::NoSuperUser),
                    Keyword::CREATEDB => (&mut builder.create_db, UserOption::CreateDB),
                    Keyword::NOCREATEDB => (&mut builder.create_db, UserOption::NoCreateDB),
                    Keyword::CREATEUSER => (&mut builder.create_user, UserOption::CreateUser),
                    Keyword::NOCREATEUSER => (&mut builder.create_user, UserOption::NoCreateUser),
                    Keyword::LOGIN => (&mut builder.login, UserOption::Login),
                    Keyword::NOLOGIN => (&mut builder.login, UserOption::NoLogin),
                    Keyword::PASSWORD => {
                        if parser.parse_keyword(Keyword::NULL) {
                            (&mut builder.password, UserOption::Password(None))
                        } else {
                            (
                                &mut builder.password,
                                UserOption::Password(Some(AstString::parse_to(parser)?)),
                            )
                        }
                    }
                    Keyword::ENCRYPTED => {
                        parser.expect_keyword(Keyword::PASSWORD)?;
                        (
                            &mut builder.password,
                            UserOption::EncryptedPassword(AstString::parse_to(parser)?),
                        )
                    }
                    _ => {
                        parser.expected(
                            "SUPERUSER | NOSUPERUSER | CREATEDB | NOCREATEDB | LOGIN \
                            | NOLOGIN | CREATEUSER | NOCREATEUSER | [ENCRYPTED] PASSWORD | NULL",
                            token,
                        )?;
                        unreachable!()
                    }
                };
                add_option(item_mut_ref, user_option)?;
            } else {
                parser.expected(
                    "SUPERUSER | NOSUPERUSER | CREATEDB | NOCREATEDB | LOGIN | NOLOGIN \
                        | CREATEUSER | NOCREATEUSER | [ENCRYPTED] PASSWORD | NULL",
                    token,
                )?
            }
        }
        Ok(builder.build())
    }
}

impl fmt::Display for UserOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if !self.0.is_empty() {
            write!(f, "WITH {}", display_separated(self.0.as_slice(), " "))
        } else {
            Ok(())
        }
    }
}

impl ParseTo for CreateUserStatement {
    fn parse_to(p: &mut Parser) -> Result<Self, ParserError> {
        impl_parse_to!(user_name: ObjectName, p);
        impl_parse_to!(with_options: UserOptions, p);

        Ok(CreateUserStatement {
            user_name,
            with_options,
        })
    }
}

impl fmt::Display for CreateUserStatement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut v: Vec<String> = vec![];
        impl_fmt_display!(user_name, v, self);
        impl_fmt_display!(with_options, v, self);
        v.iter().join(" ").fmt(f)
    }
}

impl fmt::Display for AlterUserMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AlterUserMode::Options(options) => {
                write!(f, "{}", options)
            }
            AlterUserMode::Rename(new_name) => {
                write!(f, "RENAME TO {}", new_name)
            }
        }
    }
}

impl fmt::Display for AlterUserStatement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut v: Vec<String> = vec![];
        impl_fmt_display!(user_name, v, self);
        impl_fmt_display!(mode, v, self);
        v.iter().join(" ").fmt(f)
    }
}

impl ParseTo for AlterUserStatement {
    fn parse_to(p: &mut Parser) -> Result<Self, ParserError> {
        impl_parse_to!(user_name: ObjectName, p);
        impl_parse_to!(mode: AlterUserMode, p);

        Ok(AlterUserStatement { user_name, mode })
    }
}

impl ParseTo for AlterUserMode {
    fn parse_to(p: &mut Parser) -> Result<Self, ParserError> {
        if p.parse_keyword(Keyword::RENAME) {
            p.expect_keyword(Keyword::TO)?;
            impl_parse_to!(new_name: ObjectName, p);
            Ok(AlterUserMode::Rename(new_name))
        } else {
            impl_parse_to!(with_options: UserOptions, p);
            Ok(AlterUserMode::Options(with_options))
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct DropStatement {
    /// The type of the object to drop: TABLE, VIEW, etc.
    pub object_type: ObjectType,
    /// An optional `IF EXISTS` clause. (Non-standard.)
    pub if_exists: bool,
    /// Object to drop.
    pub object_name: ObjectName,
    /// Whether `CASCADE` was specified. This will be `false` when
    /// `RESTRICT` or no drop behavior at all was specified.
    pub drop_mode: AstOption<DropMode>,
}

// sql_grammar!(DropStatement {
//     object_type: ObjectType,
//     if_exists => [Keyword::IF, Keyword::EXISTS],
//     name: ObjectName,
//     drop_mode: AstOption<DropMode>,
// });
impl ParseTo for DropStatement {
    fn parse_to(p: &mut Parser) -> Result<Self, ParserError> {
        impl_parse_to!(object_type: ObjectType, p);
        impl_parse_to!(if_exists => [Keyword::IF, Keyword::EXISTS], p);
        let object_name = p.parse_object_name()?;
        impl_parse_to!(drop_mode: AstOption<DropMode>, p);
        Ok(Self {
            object_type,
            if_exists,
            object_name,
            drop_mode,
        })
    }
}

impl fmt::Display for DropStatement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut v: Vec<String> = vec![];
        impl_fmt_display!(object_type, v, self);
        impl_fmt_display!(if_exists => [Keyword::IF, Keyword::EXISTS], v, self);
        impl_fmt_display!(object_name, v, self);
        impl_fmt_display!(drop_mode, v, self);
        v.iter().join(" ").fmt(f)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum DropMode {
    Cascade,
    Restrict,
}

impl ParseTo for DropMode {
    fn parse_to(parser: &mut Parser) -> Result<Self, ParserError> {
        let drop_mode = if parser.parse_keyword(Keyword::CASCADE) {
            DropMode::Cascade
        } else if parser.parse_keyword(Keyword::RESTRICT) {
            DropMode::Restrict
        } else {
            return parser.expected("CASCADE | RESTRICT", parser.peek_token());
        };
        Ok(drop_mode)
    }
}

impl fmt::Display for DropMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            DropMode::Cascade => "CASCADE",
            DropMode::Restrict => "RESTRICT",
        })
    }
}
