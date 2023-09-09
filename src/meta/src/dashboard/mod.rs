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

mod prometheus;
mod proxy;

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::{anyhow, Result};
use axum::body::Body;
use axum::extract::{Extension, Path};
use axum::http::{Method, StatusCode};
use axum::response::IntoResponse;
use axum::routing::{get, get_service};
use axum::Router;
use hyper::Request;
use parking_lot::Mutex;
use risingwave_rpc_client::ComputeClientPool;
use tower::ServiceBuilder;
use tower_http::add_extension::AddExtensionLayer;
use tower_http::cors::{self, CorsLayer};
use tower_http::services::ServeDir;

use crate::manager::{ClusterManagerRef, FragmentManagerRef};
use crate::storage::MetaStoreRef;

#[derive(Clone)]
pub struct DashboardService {
    pub dashboard_addr: SocketAddr,
    pub prometheus_endpoint: Option<String>,
    pub prometheus_client: Option<prometheus_http_query::Client>,
    pub cluster_manager: ClusterManagerRef,
    pub fragment_manager: FragmentManagerRef,
    pub compute_clients: ComputeClientPool,

    // TODO: replace with catalog manager.
    pub meta_store: MetaStoreRef,
}

pub type Service = Arc<DashboardService>;

pub(super) mod handlers {
    use anyhow::Context;
    use axum::Json;
    use itertools::Itertools;
    use risingwave_pb::catalog::table::TableType;
    use risingwave_pb::catalog::{Sink, Source, Table};
    use risingwave_pb::common::WorkerNode;
    use risingwave_pb::meta::{ActorLocation, PbTableFragments};
    use risingwave_pb::monitor_service::StackTraceResponse;
    use serde_json::json;

    use super::*;
    use crate::manager::WorkerId;
    use crate::model::TableFragments;
    use crate::storage::MetaStoreRef;

    pub struct DashboardError(anyhow::Error);
    pub type Result<T> = std::result::Result<T, DashboardError>;

    pub fn err(err: impl Into<anyhow::Error>) -> DashboardError {
        DashboardError(err.into())
    }

    impl IntoResponse for DashboardError {
        fn into_response(self) -> axum::response::Response {
            let mut resp = Json(json!({
                "error": format!("{}", self.0),
                "info":  format!("{:?}", self.0),
            }))
            .into_response();
            *resp.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;
            resp
        }
    }

    pub async fn list_clusters(
        Path(ty): Path<i32>,
        Extension(srv): Extension<Service>,
    ) -> Result<Json<Vec<WorkerNode>>> {
        use risingwave_pb::common::WorkerType;
        let mut result = srv
            .cluster_manager
            .list_worker_node(
                WorkerType::from_i32(ty)
                    .ok_or_else(|| anyhow!("invalid worker type"))
                    .map_err(err)?,
                None,
            )
            .await;
        result.sort_unstable_by_key(|n| n.id);
        Ok(result.into())
    }

    async fn list_table_catalogs_inner(
        meta_store: &MetaStoreRef,
        table_type: TableType,
    ) -> Result<Json<Vec<Table>>> {
        use crate::model::MetadataModel;

        let results = Table::list(meta_store)
            .await
            .map_err(err)?
            .into_iter()
            .filter(|t| t.table_type() == table_type)
            .collect();

        Ok(Json(results))
    }

    pub async fn list_materialized_views(
        Extension(srv): Extension<Service>,
    ) -> Result<Json<Vec<Table>>> {
        list_table_catalogs_inner(&srv.meta_store, TableType::MaterializedView).await
    }

    pub async fn list_tables(Extension(srv): Extension<Service>) -> Result<Json<Vec<Table>>> {
        list_table_catalogs_inner(&srv.meta_store, TableType::Table).await
    }

    pub async fn list_indexes(Extension(srv): Extension<Service>) -> Result<Json<Vec<Table>>> {
        list_table_catalogs_inner(&srv.meta_store, TableType::Index).await
    }

    pub async fn list_internal_tables(
        Extension(srv): Extension<Service>,
    ) -> Result<Json<Vec<Table>>> {
        list_table_catalogs_inner(&srv.meta_store, TableType::Internal).await
    }

    pub async fn list_sources(Extension(srv): Extension<Service>) -> Result<Json<Vec<Source>>> {
        use crate::model::MetadataModel;

        let sources = Source::list(&srv.meta_store).await.map_err(err)?;
        Ok(Json(sources))
    }

    pub async fn list_sinks(Extension(srv): Extension<Service>) -> Result<Json<Vec<Sink>>> {
        use crate::model::MetadataModel;

        let sinks = Sink::list(&srv.meta_store).await.map_err(err)?;
        Ok(Json(sinks))
    }

    pub async fn list_actors(
        Extension(srv): Extension<Service>,
    ) -> Result<Json<Vec<ActorLocation>>> {
        let mut node_actors = srv.fragment_manager.all_node_actors(true).await;
        let nodes = srv
            .cluster_manager
            .list_active_streaming_compute_nodes()
            .await;
        let actors = nodes
            .into_iter()
            .map(|node| ActorLocation {
                node: Some(node.clone()),
                actors: node_actors.remove(&node.id).unwrap_or_default(),
            })
            .collect::<Vec<_>>();

        Ok(Json(actors))
    }

    pub async fn list_fragments(
        Extension(srv): Extension<Service>,
    ) -> Result<Json<Vec<PbTableFragments>>> {
        use crate::model::MetadataModel;

        let table_fragments = TableFragments::list(&srv.meta_store)
            .await
            .map_err(err)?
            .into_iter()
            .map(|x| x.to_protobuf())
            .collect_vec();
        Ok(Json(table_fragments))
    }

    pub async fn dump_await_tree(
        Path(worker_id): Path<WorkerId>,
        Extension(srv): Extension<Service>,
    ) -> Result<Json<StackTraceResponse>> {
        let worker_node = srv
            .cluster_manager
            .get_worker_by_id(worker_id)
            .await
            .context("worker node not found")
            .map_err(err)?
            .worker_node;

        let client = srv.compute_clients.get(&worker_node).await.map_err(err)?;

        let result = client.stack_trace().await.map_err(err)?;

        Ok(result.into())
    }
}

impl DashboardService {
    pub async fn serve(self, ui_path: Option<String>) -> Result<()> {
        use handlers::*;
        let srv = Arc::new(self);

        let cors_layer = CorsLayer::new()
            .allow_origin(cors::Any)
            .allow_methods(vec![Method::GET]);

        let api_router = Router::new()
            .route("/clusters/:ty", get(list_clusters))
            .route("/actors", get(list_actors))
            .route("/fragments2", get(list_fragments))
            .route("/materialized_views", get(list_materialized_views))
            .route("/tables", get(list_tables))
            .route("/indexes", get(list_indexes))
            .route("/internal_tables", get(list_internal_tables))
            .route("/sources", get(list_sources))
            .route("/sinks", get(list_sinks))
            .route("/metrics/cluster", get(prometheus::list_prometheus_cluster))
            .route(
                "/metrics/actor/back_pressures",
                get(prometheus::list_prometheus_actor_back_pressure),
            )
            .route("/monitor/await_tree/:worker_id", get(dump_await_tree))
            .layer(
                ServiceBuilder::new()
                    .layer(AddExtensionLayer::new(srv.clone()))
                    .into_inner(),
            )
            .layer(cors_layer);

        let app = if let Some(ui_path) = ui_path {
            let static_file_router = Router::new().nest_service(
                "/",
                get_service(ServeDir::new(ui_path)).handle_error(|e| async move {
                    (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        format!("Unhandled internal error: {e}",),
                    )
                }),
            );
            Router::new()
                .fallback_service(static_file_router)
                .nest("/api", api_router)
        } else {
            let cache = Arc::new(Mutex::new(HashMap::new()));
            let service = tower::service_fn(move |req: Request<Body>| {
                let cache = cache.clone();
                async move {
                    proxy::proxy(req, cache).await.or_else(|err| {
                        Ok((
                            StatusCode::INTERNAL_SERVER_ERROR,
                            format!("Unhandled internal error: {}", err),
                        )
                            .into_response())
                    })
                }
            });
            Router::new()
                .fallback_service(service)
                .nest("/api", api_router)
        };

        axum::Server::bind(&srv.dashboard_addr)
            .serve(app.into_make_service())
            .await
            .map_err(|err| anyhow!(err))?;
        Ok(())
    }
}
