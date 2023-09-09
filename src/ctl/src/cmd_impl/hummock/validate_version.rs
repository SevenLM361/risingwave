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

use risingwave_hummock_sdk::compaction_group::hummock_version_ext;
use risingwave_rpc_client::HummockMetaClient;

use crate::CtlContext;

pub async fn validate_version(context: &CtlContext) -> anyhow::Result<()> {
    let meta_client = context.meta_client().await?;
    let version = meta_client.get_current_version().await?;
    let result = hummock_version_ext::validate_version(&version);
    if !result.is_empty() {
        println!("Invalid HummockVersion. Violation lists:");
        for s in result {
            println!("{}", s);
        }
    }

    Ok(())
}
