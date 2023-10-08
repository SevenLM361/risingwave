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

use std::collections::HashMap;
use std::rc::Rc;
use std::sync::LazyLock;

use anyhow::Context;
use itertools::Itertools;
use maplit::{convert_args, hashmap};
use pgwire::pg_response::{PgResponse, StatementType};
use risingwave_common::catalog::{ConnectionId, DatabaseId, SchemaId, UserId};
use risingwave_common::error::{ErrorCode, Result};
use risingwave_common::util::column_index_mapping::ColIndexMapping;
use risingwave_connector::sink::catalog::{SinkCatalog, SinkFormatDesc};
use risingwave_connector::sink::{
    CONNECTOR_TYPE_KEY, SINK_TYPE_OPTION, SINK_USER_FORCE_APPEND_ONLY_OPTION,
};
use risingwave_pb::catalog::table::OptionalAssociatedSourceId;
use risingwave_pb::catalog::Table;
use risingwave_pb::stream_plan::stream_fragment_graph::{Parallelism, StreamFragment};
use risingwave_pb::stream_plan::stream_node::NodeBody;
use risingwave_pb::stream_plan::{DispatcherType, MergeNode, StreamFragmentGraph, StreamNode};
use risingwave_sqlparser::ast::{
    CreateSink, CreateSinkStatement, EmitMode, Encode, Format, ObjectName, Query, Select,
    SelectItem, SetExpr, SinkSchema, Statement, TableFactor, TableWithJoins,
};
use risingwave_sqlparser::parser::Parser;

use super::create_mv::get_column_names;
use super::RwPgResponse;
use crate::binder::Binder;
use crate::catalog::root_catalog::SchemaPath;
use crate::handler::create_table::{
    gen_create_table_plan, gen_create_table_plan_with_source, ColumnIdGenerator,
};
use crate::handler::privilege::resolve_query_privileges;
use crate::handler::HandlerArgs;
use crate::optimizer::plan_node::{Explain, StreamExchange};
use crate::optimizer::{OptimizerContext, OptimizerContextRef, PlanRef, RelationCollectorVisitor};
use crate::scheduler::streaming_manager::CreatingStreamingJobInfo;
use crate::session::SessionImpl;
use crate::stream_fragmenter::build_graph;
use crate::utils::resolve_privatelink_in_with_option;
use crate::{Planner, TableCatalog, WithOptions};

pub fn gen_sink_query_from_name(from_name: ObjectName) -> Result<Query> {
    let table_factor = TableFactor::Table {
        name: from_name,
        alias: None,
        for_system_time_as_of_proctime: false,
    };
    let from = vec![TableWithJoins {
        relation: table_factor,
        joins: vec![],
    }];
    let select = Select {
        from,
        projection: vec![SelectItem::Wildcard(None)],
        ..Default::default()
    };
    let body = SetExpr::Select(Box::new(select));
    Ok(Query {
        with: None,
        body,
        order_by: vec![],
        limit: None,
        offset: None,
        fetch: None,
    })
}

pub fn gen_sink_plan(
    session: &SessionImpl,
    context: OptimizerContextRef,
    stmt: CreateSinkStatement,
) -> Result<(Box<Query>, PlanRef, SinkCatalog)> {
    let db_name = session.database();
    let (sink_schema_name, sink_table_name) =
        Binder::resolve_schema_qualified_name(db_name, stmt.sink_name.clone())?;

    // Used for debezium's table name
    let sink_from_table_name;
    let query = match stmt.sink_from {
        CreateSink::From(from_name) => {
            sink_from_table_name = from_name.0.last().unwrap().real_value();
            Box::new(gen_sink_query_from_name(from_name)?)
        }
        CreateSink::AsQuery(query) => {
            sink_from_table_name = sink_table_name.clone();
            query
        }
    };

    let sink_into_table_name = stmt.into_table_name.map(|name| name.real_value());

    let (sink_database_id, sink_schema_id) =
        session.get_database_and_schema_id_for_create(sink_schema_name.clone())?;

    let definition = context.normalized_sql().to_owned();

    let (dependent_relations, bound) = {
        let mut binder = Binder::new_for_stream(session);
        let bound = binder.bind_query(*query.clone())?;
        (binder.included_relations(), bound)
    };

    let check_items = resolve_query_privileges(&bound);
    session.check_privileges(&check_items)?;

    // If column names not specified, use the name in materialized view.
    let col_names = get_column_names(&bound, session, stmt.columns)?;

    let mut with_options = context.with_options().clone();
    let connection_id = {
        let conn_id =
            resolve_privatelink_in_with_option(&mut with_options, &sink_schema_name, session)?;
        conn_id.map(ConnectionId)
    };

    let emit_on_window_close = stmt.emit_mode == Some(EmitMode::OnWindowClose);
    if emit_on_window_close {
        context.warn_to_user("EMIT ON WINDOW CLOSE is currently an experimental feature. Please use it with caution.");
    }

    let format_desc = if let Some(connector) = with_options.get(CONNECTOR_TYPE_KEY) {
        match stmt.sink_schema {
            // Case A: new syntax `format ... encode ...`
            Some(f) => {
                validate_compatibility(connector, &f)?;
                Some(bind_sink_format_desc(f)?)
            },
            None => match with_options.get(SINK_TYPE_OPTION) {
                // Case B: old syntax `type = '...'`
                Some(t) => SinkFormatDesc::from_legacy_type(connector, t)?.map(|mut f| {
                    session.notice_to_user("Consider using the newer syntax `FORMAT ... ENCODE ...` instead of `type = '...'`.");
                    if let Some(v) = with_options.get(SINK_USER_FORCE_APPEND_ONLY_OPTION) {
                        f.options.insert(SINK_USER_FORCE_APPEND_ONLY_OPTION.into(), v.into());
                    }
                    f
                }),
                // Case C: no format + encode required
                None => None,
            },
        }
    } else {
        None
    };

    // let connector = with_options
    //     .get(CONNECTOR_TYPE_KEY)
    //     .ok_or_else(|| ErrorCode::BindError(format!("missing field '{CONNECTOR_TYPE_KEY}'")))?;

    let mut plan_root = Planner::new(context).plan_query(bound)?;
    if let Some(col_names) = col_names {
        plan_root.set_out_names(col_names)?;
    };

    let sink_plan = plan_root.gen_sink_plan(
        sink_table_name,
        definition,
        with_options,
        emit_on_window_close,
        db_name.to_owned(),
        sink_from_table_name,
        format_desc,
        sink_into_table_name,
    )?;
    let sink_desc = sink_plan.sink_desc().clone();
    let sink_plan: PlanRef = sink_plan.into();

    let ctx = sink_plan.ctx();
    let explain_trace = ctx.is_explain_trace();
    if explain_trace {
        ctx.trace("Create Sink:");
        ctx.trace(sink_plan.explain_to_string());
    }

    let dependent_relations =
        RelationCollectorVisitor::collect_with(dependent_relations, sink_plan.clone());

    let sink_catalog = sink_desc.into_catalog(
        SchemaId::new(sink_schema_id),
        DatabaseId::new(sink_database_id),
        UserId::new(session.user_id()),
        connection_id,
        dependent_relations.into_iter().collect_vec(),
    );

    Ok((query, sink_plan, sink_catalog))
}

pub async fn handle_create_sink(
    handle_args: HandlerArgs,
    stmt: CreateSinkStatement,
) -> Result<RwPgResponse> {
    let session = handle_args.session.clone();

    session.check_relation_name_duplicated(stmt.sink_name.clone())?;

    let target_table_name = stmt.into_table_name.clone();

    let (sink, sink_graph) = {
        let context = Rc::new(OptimizerContext::from_handler_args(handle_args));
        let (query, plan, sink) = gen_sink_plan(&session, context.clone(), stmt.clone())?;
        let has_order_by = !query.order_by.is_empty();
        if has_order_by {
            context.warn_to_user(
                r#"The ORDER BY clause in the CREATE SINK statement has no effect at all."#
                    .to_string(),
            );
        }
        let mut graph = build_graph(plan.clone());
        graph.parallelism = session
            .config()
            .get_streaming_parallelism()
            .map(|parallelism| Parallelism { parallelism });
        (sink, graph)
    };

    if let Some(table_name) = target_table_name {
        // copy start
        let db_name = session.database();
        let (schema_name, real_table_name) =
            Binder::resolve_schema_qualified_name(db_name, table_name.clone())?;
        let search_path = session.config().get_search_path();
        let user_name = &session.auth_context().user_name;

        let schema_path = SchemaPath::new(schema_name.as_deref(), &search_path, user_name);

        let original_catalog = {
            let reader = session.env().catalog_reader().read_guard();
            let (table, schema_name) =
                reader.get_table_by_name(db_name, schema_path, &real_table_name)?;

            session.check_privilege_for_drop_alter(schema_name, &**table)?;

            table.clone()
        };

        // Retrieve the original table definition and parse it to AST.
        let [definition]: [_; 1] = Parser::parse_sql(&original_catalog.definition)
            .context("unable to parse original table definition")?
            .try_into()
            .unwrap();

        // Create handler args as if we're creating a new table with the altered definition.
        let handler_args = HandlerArgs::new(session.clone(), &definition, "")?;
        let col_id_gen = ColumnIdGenerator::new_alter(&original_catalog);

        let Statement::CreateTable {
            or_replace,
            temporary,
            if_not_exists,
            name,
            columns,
            constraints,
            with_options,
            source_schema,
            source_watermarks,
            append_only,
            query,
        } = definition
        else {
            panic!("unexpected statement: {:?}", definition);
        };

        let source_schema = source_schema
            .clone()
            .map(|source_schema| source_schema.into_source_schema_v2().0);

        let (graph, table, source) = {
            let context = OptimizerContext::from_handler_args(handler_args);
            let (plan, source, table) = match source_schema {
                Some(source_schema) => {
                    gen_create_table_plan_with_source(
                        context,
                        table_name,
                        columns,
                        constraints,
                        source_schema,
                        source_watermarks,
                        col_id_gen,
                        append_only,
                    )
                    .await?
                }
                None => gen_create_table_plan(
                    context,
                    table_name,
                    columns,
                    constraints,
                    col_id_gen,
                    source_watermarks,
                    append_only,
                )?,
            };

            //            let (_, plan, _) = gen_sink_plan(&session, context.clone(), stmt)?;
            let mut mat = plan.as_stream_materialize().cloned().unwrap();

            // let plan = plan.clone().as_stream_materialize().as_mut().unwrap();

            // // TODO: avoid this backward conversion.
            // if TableCatalog::from(&table).pk_column_ids() != original_catalog.pk_column_ids() {
            //     Err(ErrorCode::InvalidInputSyntax(
            //         "alter primary key of table is not supported".to_owned(),
            //     ))?
            // }

            let mut graph = StreamFragmentGraph {
                parallelism: session
                    .config()
                    .get_streaming_parallelism()
                    .map(|parallelism| Parallelism { parallelism }),
                ..build_graph(plan)
            };

            fn dfs(node: &StreamNode, depth: i32) {
                let x = match &node.node_body {
                    None => "".to_string(),
                    Some(x) => x.to_string(),
                };

                println!("{}{} {}", " ".repeat(depth as usize), node.identity, x);

                for input in &node.input {
                    dfs(input, depth + 1);
                }
                // dfs(&node.left, depth + 1);
                // dfs(&node.right, depth + 1);
                //    }
            }

            let max_fragment_id = graph.fragments.keys().max().cloned().unwrap();

            for (id, fragment) in graph.fragments.iter_mut() {
                println!("id {}, {}", id, fragment.fragment_type_mask);

                if let Some(x) = &mut fragment.node {
                    if let Some(NodeBody::Source(s)) = &x.node_body && s.source_inner.is_none() {
                        x.node_body = Some(NodeBody::Merge(MergeNode{
                            upstream_actor_id: vec![],
                            upstream_fragment_id: 0,
                            upstream_dispatcher_type: DispatcherType::Unspecified.into() ,
                            fields: vec![],
                        }));
                    }
                }
                // fragment.

                // if let Some(node) = &fragment.node {
                //     if let Some(NodeBody::D) = &node.node_body {
                //
                //     }
                // }
            }
            // StreamFragment {
            //     fragment_id: max_fragment_id + 1,
            //     node: StreamNode {
            //         operator_id: 0,
            //         input: vec![],
            //         stream_key: vec![],
            //         append_only,
            //         identity: "".to_string(),
            //         fields: vec![],
            //         node_body: None,
            //     },
            //     fragment_type_mask: 0,
            //     requires_singleton: false,
            //     table_ids_cnt: 0,
            //     upstream_table_ids: vec![],
            // };

            // Fill the original table ID.
            let table = Table {
                id: original_catalog.id().table_id(),
                optional_associated_source_id: original_catalog.associated_source_id().map(
                    |source_id| OptionalAssociatedSourceId::AssociatedSourceId(source_id.into()),
                ),
                ..table
            };

            (graph, table, source)
        };

        // Calculate the mapping from the original columns to the new columns.
        let col_index_mapping = ColIndexMapping::with_target_size(
            original_catalog
                .columns()
                .iter()
                .map(|old_c| {
                    table.columns.iter().position(|new_c| {
                        new_c.get_column_desc().unwrap().column_id == old_c.column_id().get_id()
                    })
                })
                .collect(),
            table.columns.len(),
        );

        let catalog_writer = session.catalog_writer()?;

        // catalog_writer
        //     .replace_table(source, table, graph, col_index_mapping)
        //     .await?;

        catalog_writer
            .create_sink_into_table(
                sink.to_proto(),
                sink_graph,
                source,
                table,
                graph,
                col_index_mapping,
            )
            .await
            .unwrap();

        return Ok(PgResponse::empty_result(StatementType::ALTER_TABLE));
        //`
    }

    let _job_guard =
        session
            .env()
            .creating_streaming_job_tracker()
            .guard(CreatingStreamingJobInfo::new(
                session.session_id(),
                sink.database_id.database_id,
                sink.schema_id.schema_id,
                sink.name.clone(),
            ));

    let catalog_writer = session.catalog_writer()?;
    catalog_writer.create_sink(sink.to_proto(), sink_graph).await?;

    Ok(PgResponse::empty_result(StatementType::CREATE_SINK))
}

/// Transforms the (format, encode, options) from sqlparser AST into an internal struct `SinkFormatDesc`.
/// This is an analogy to (part of) [`crate::handler::create_source::try_bind_columns_from_source`]
/// which transforms sqlparser AST `SourceSchemaV2` into `StreamSourceInfo`.
fn bind_sink_format_desc(value: SinkSchema) -> Result<SinkFormatDesc> {
    use risingwave_connector::sink::catalog::{SinkEncode, SinkFormat};
    use risingwave_sqlparser::ast::{Encode as E, Format as F};

    let format = match value.format {
        F::Plain => SinkFormat::AppendOnly,
        F::Upsert => SinkFormat::Upsert,
        F::Debezium => SinkFormat::Debezium,
        f @ (F::Native | F::DebeziumMongo | F::Maxwell | F::Canal) => {
            return Err(ErrorCode::BindError(format!("sink format unsupported: {f}")).into())
        }
    };
    let encode = match value.row_encode {
        E::Json => SinkEncode::Json,
        E::Protobuf => SinkEncode::Protobuf,
        E::Avro => SinkEncode::Avro,
        e @ (E::Native | E::Csv | E::Bytes) => {
            return Err(ErrorCode::BindError(format!("sink encode unsupported: {e}")).into())
        }
    };
    let options = WithOptions::try_from(value.row_options.as_slice())?.into_inner();

    Ok(SinkFormatDesc {
        format,
        encode,
        options,
    })
}

static CONNECTORS_COMPATIBLE_FORMATS: LazyLock<HashMap<String, HashMap<Format, Vec<Encode>>>> =
    LazyLock::new(|| {
        use risingwave_connector::sink::kafka::KafkaSink;
        use risingwave_connector::sink::kinesis::KinesisSink;
        use risingwave_connector::sink::pulsar::PulsarSink;
        use risingwave_connector::sink::Sink as _;

        convert_args!(hashmap!(
                KafkaSink::SINK_NAME => hashmap!(
                    Format::Plain => vec![Encode::Json],
                    Format::Upsert => vec![Encode::Json],
                    Format::Debezium => vec![Encode::Json],
                ),
                KinesisSink::SINK_NAME => hashmap!(
                    Format::Plain => vec![Encode::Json],
                    Format::Upsert => vec![Encode::Json],
                    Format::Debezium => vec![Encode::Json],
                ),
                PulsarSink::SINK_NAME => hashmap!(
                    Format::Plain => vec![Encode::Json],
                    Format::Upsert => vec![Encode::Json],
                    Format::Debezium => vec![Encode::Json],
                ),
        ))
    });
pub fn validate_compatibility(connector: &str, format_desc: &SinkSchema) -> Result<()> {
    let compatible_formats = CONNECTORS_COMPATIBLE_FORMATS
        .get(connector)
        .ok_or_else(|| {
            ErrorCode::BindError(format!(
                "connector {} is not supported by FORMAT ... ENCODE ... syntax",
                connector
            ))
        })?;
    let compatible_encodes = compatible_formats.get(&format_desc.format).ok_or_else(|| {
        ErrorCode::BindError(format!(
            "connector {} does not support format {:?}",
            connector, format_desc.format
        ))
    })?;
    if !compatible_encodes.contains(&format_desc.row_encode) {
        return Err(ErrorCode::BindError(format!(
            "connector {} does not support format {:?} with encode {:?}",
            connector, format_desc.format, format_desc.row_encode
        ))
        .into());
    }
    Ok(())
}

/// For `planner_test` crate so that it does not depend directly on `connector` crate just for `SinkFormatDesc`.
impl TryFrom<&WithOptions> for Option<SinkFormatDesc> {
    type Error = risingwave_connector::sink::SinkError;

    fn try_from(value: &WithOptions) -> std::result::Result<Self, Self::Error> {
        let connector = value.get(CONNECTOR_TYPE_KEY);
        let r#type = value.get(SINK_TYPE_OPTION);
        match (connector, r#type) {
            (Some(c), Some(t)) => SinkFormatDesc::from_legacy_type(c, t),
            _ => Ok(None),
        }
    }
}

#[cfg(test)]
pub mod tests {
    use risingwave_common::catalog::{DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME};

    use crate::catalog::root_catalog::SchemaPath;
    use crate::test_utils::{create_proto_file, LocalFrontend, PROTO_FILE_DATA};

    #[tokio::test]
    async fn test_create_sink_handler() {
        let proto_file = create_proto_file(PROTO_FILE_DATA);
        let sql = format!(
            r#"CREATE SOURCE t1
    WITH (connector = 'kafka', kafka.topic = 'abc', kafka.servers = 'localhost:1001')
    FORMAT PLAIN ENCODE PROTOBUF (message = '.test.TestRecord', schema.location = 'file://{}')"#,
            proto_file.path().to_str().unwrap()
        );
        let frontend = LocalFrontend::new(Default::default()).await;
        frontend.run_sql(sql).await.unwrap();

        let sql = "create materialized view mv1 as select t1.country from t1;";
        frontend.run_sql(sql).await.unwrap();

        let sql = r#"CREATE SINK snk1 FROM mv1
                    WITH (connector = 'mysql', mysql.endpoint = '127.0.0.1:3306', mysql.table =
                        '<table_name>', mysql.database = '<database_name>', mysql.user = '<user_name>',
                        mysql.password = '<password>', type = 'append-only', force_append_only = 'true');"#.to_string();
        frontend.run_sql(sql).await.unwrap();

        let session = frontend.session_ref();
        let catalog_reader = session.env().catalog_reader().read_guard();
        let schema_path = SchemaPath::Name(DEFAULT_SCHEMA_NAME);

        // Check source exists.
        let (source, _) = catalog_reader
            .get_source_by_name(DEFAULT_DATABASE_NAME, schema_path, "t1")
            .unwrap();
        assert_eq!(source.name, "t1");

        // Check table exists.
        let (table, schema_name) = catalog_reader
            .get_table_by_name(DEFAULT_DATABASE_NAME, schema_path, "mv1")
            .unwrap();
        assert_eq!(table.name(), "mv1");

        // Check sink exists.
        let (sink, _) = catalog_reader
            .get_sink_by_name(DEFAULT_DATABASE_NAME, SchemaPath::Name(schema_name), "snk1")
            .unwrap();
        assert_eq!(sink.name, "snk1");
    }
}
