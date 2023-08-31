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

use std::borrow::Cow;
use std::collections::HashMap;
use std::time::Duration;

use anyhow::Ok;
use aws_sdk_kinesis::Client as KinesisClient;
use clickhouse::Client as ClickHouseClient;
use rdkafka::ClientConfig;
use redis::Client as RedisClient;
use serde_derive::{Deserialize, Serialize};
use serde_with::json::JsonString;
use serde_with::{serde_as, DisplayFromStr};

use crate::aws_auth::AwsAuthProps;
use crate::deserialize_duration_from_string;

// The file describes the common abstractions for each connector and can be used in both source and
// sink.

pub const BROKER_REWRITE_MAP_KEY: &str = "broker.rewrite.endpoints";
pub const PRIVATE_LINK_TARGETS_KEY: &str = "privatelink.targets";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AwsPrivateLinkItem {
    pub az_id: Option<String>,
    pub port: u16,
}

#[serde_as]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaCommon {
    #[serde(rename = "properties.bootstrap.server", alias = "kafka.brokers")]
    pub brokers: String,

    #[serde(rename = "broker.rewrite.endpoints")]
    #[serde_as(as = "Option<JsonString>")]
    pub broker_rewrite_map: Option<HashMap<String, String>>,

    #[serde(rename = "topic", alias = "kafka.topic")]
    pub topic: String,

    #[serde(
        rename = "properties.sync.call.timeout",
        deserialize_with = "deserialize_duration_from_string",
        default = "default_kafka_sync_call_timeout"
    )]
    pub sync_call_timeout: Duration,

    /// Security protocol used for RisingWave to communicate with Kafka brokers. Could be
    /// PLAINTEXT, SSL, SASL_PLAINTEXT or SASL_SSL.
    #[serde(rename = "properties.security.protocol")]
    security_protocol: Option<String>,

    // For the properties below, please refer to [librdkafka](https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md) for more information.
    /// Path to CA certificate file for verifying the broker's key.
    #[serde(rename = "properties.ssl.ca.location")]
    ssl_ca_location: Option<String>,

    /// Path to client's certificate file (PEM).
    #[serde(rename = "properties.ssl.certificate.location")]
    ssl_certificate_location: Option<String>,

    /// Path to client's private key file (PEM).
    #[serde(rename = "properties.ssl.key.location")]
    ssl_key_location: Option<String>,

    /// Passphrase of client's private key.
    #[serde(rename = "properties.ssl.key.password")]
    ssl_key_password: Option<String>,

    /// SASL mechanism if SASL is enabled. Currently support PLAIN, SCRAM and GSSAPI.
    #[serde(rename = "properties.sasl.mechanism")]
    sasl_mechanism: Option<String>,

    /// SASL username for SASL/PLAIN and SASL/SCRAM.
    #[serde(rename = "properties.sasl.username")]
    sasl_username: Option<String>,

    /// SASL password for SASL/PLAIN and SASL/SCRAM.
    #[serde(rename = "properties.sasl.password")]
    sasl_password: Option<String>,

    /// Kafka server's Kerberos principal name under SASL/GSSAPI, not including /hostname@REALM.
    #[serde(rename = "properties.sasl.kerberos.service.name")]
    sasl_kerberos_service_name: Option<String>,

    /// Path to client's Kerberos keytab file under SASL/GSSAPI.
    #[serde(rename = "properties.sasl.kerberos.keytab")]
    sasl_kerberos_keytab: Option<String>,

    /// Client's Kerberos principal name under SASL/GSSAPI.
    #[serde(rename = "properties.sasl.kerberos.principal")]
    sasl_kerberos_principal: Option<String>,

    /// Shell command to refresh or acquire the client's Kerberos ticket under SASL/GSSAPI.
    #[serde(rename = "properties.sasl.kerberos.kinit.cmd")]
    sasl_kerberos_kinit_cmd: Option<String>,

    /// Minimum time in milliseconds between key refresh attempts under SASL/GSSAPI.
    #[serde(rename = "properties.sasl.kerberos.min.time.before.relogin")]
    sasl_kerberos_min_time_before_relogin: Option<String>,

    /// Configurations for SASL/OAUTHBEARER.
    #[serde(rename = "properties.sasl.oauthbearer.config")]
    sasl_oathbearer_config: Option<String>,

    #[serde(flatten)]
    pub rdkafka_properties: RdKafkaPropertiesCommon,
}

const fn default_kafka_sync_call_timeout() -> Duration {
    Duration::from_secs(5)
}

#[serde_as]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RdKafkaPropertiesCommon {
    /// Maximum Kafka protocol request message size. Due to differing framing overhead between
    /// protocol versions the producer is unable to reliably enforce a strict max message limit at
    /// produce time and may exceed the maximum size by one message in protocol ProduceRequests,
    /// the broker will enforce the the topic's max.message.bytes limit
    #[serde(rename = "properties.message.max.bytes")]
    #[serde_as(as = "Option<DisplayFromStr>")]
    pub message_max_bytes: Option<usize>,

    /// Maximum Kafka protocol response message size. This serves as a safety precaution to avoid
    /// memory exhaustion in case of protocol hickups. This value must be at least fetch.max.bytes
    /// + 512 to allow for protocol overhead; the value is adjusted automatically unless the
    /// configuration property is explicitly set.
    #[serde(rename = "properties.receive.message.max.bytes")]
    #[serde_as(as = "Option<DisplayFromStr>")]
    pub receive_message_max_bytes: Option<usize>,
}

impl RdKafkaPropertiesCommon {
    pub(crate) fn set_client(&self, c: &mut rdkafka::ClientConfig) {
        if let Some(v) = self.message_max_bytes {
            c.set("message.max.bytes", v.to_string());
        }
        if let Some(v) = self.message_max_bytes {
            c.set("receive.message.max.bytes", v.to_string());
        }
    }
}

impl KafkaCommon {
    pub(crate) fn set_security_properties(&self, config: &mut ClientConfig) {
        // Security protocol
        if let Some(security_protocol) = self.security_protocol.as_ref() {
            config.set("security.protocol", security_protocol);
        }

        // SSL
        if let Some(ssl_ca_location) = self.ssl_ca_location.as_ref() {
            config.set("ssl.ca.location", ssl_ca_location);
        }
        if let Some(ssl_certificate_location) = self.ssl_certificate_location.as_ref() {
            config.set("ssl.certificate.location", ssl_certificate_location);
        }
        if let Some(ssl_key_location) = self.ssl_key_location.as_ref() {
            config.set("ssl.key.location", ssl_key_location);
        }
        if let Some(ssl_key_password) = self.ssl_key_password.as_ref() {
            config.set("ssl.key.password", ssl_key_password);
        }

        // SASL mechanism
        if let Some(sasl_mechanism) = self.sasl_mechanism.as_ref() {
            config.set("sasl.mechanism", sasl_mechanism);
        }

        // SASL/PLAIN & SASL/SCRAM
        if let Some(sasl_username) = self.sasl_username.as_ref() {
            config.set("sasl.username", sasl_username);
        }
        if let Some(sasl_password) = self.sasl_password.as_ref() {
            config.set("sasl.password", sasl_password);
        }

        // SASL/GSSAPI
        if let Some(sasl_kerberos_service_name) = self.sasl_kerberos_service_name.as_ref() {
            config.set("sasl.kerberos.service.name", sasl_kerberos_service_name);
        }
        if let Some(sasl_kerberos_keytab) = self.sasl_kerberos_keytab.as_ref() {
            config.set("sasl.kerberos.keytab", sasl_kerberos_keytab);
        }
        if let Some(sasl_kerberos_principal) = self.sasl_kerberos_principal.as_ref() {
            config.set("sasl.kerberos.principal", sasl_kerberos_principal);
        }
        if let Some(sasl_kerberos_kinit_cmd) = self.sasl_kerberos_kinit_cmd.as_ref() {
            config.set("sasl.kerberos.kinit.cmd", sasl_kerberos_kinit_cmd);
        }
        if let Some(sasl_kerberos_min_time_before_relogin) =
            self.sasl_kerberos_min_time_before_relogin.as_ref()
        {
            config.set(
                "sasl.kerberos.min.time.before.relogin",
                sasl_kerberos_min_time_before_relogin,
            );
        }

        // SASL/OAUTHBEARER
        if let Some(sasl_oathbearer_config) = self.sasl_oathbearer_config.as_ref() {
            config.set("sasl.oauthbearer.config", sasl_oathbearer_config);
        }
        // Currently, we only support unsecured OAUTH.
        config.set("enable.sasl.oauthbearer.unsecure.jwt", "true");
    }

    pub(crate) fn set_client(&self, c: &mut rdkafka::ClientConfig) {
        self.rdkafka_properties.set_client(c);
    }
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct KinesisCommon {
    #[serde(rename = "stream", alias = "kinesis.stream.name")]
    pub stream_name: String,
    #[serde(rename = "aws.region", alias = "kinesis.stream.region")]
    pub stream_region: String,
    #[serde(rename = "endpoint", alias = "kinesis.endpoint")]
    pub endpoint: Option<String>,
    #[serde(
        rename = "aws.credentials.access_key_id",
        alias = "kinesis.credentials.access"
    )]
    pub credentials_access_key: Option<String>,
    #[serde(
        rename = "aws.credentials.secret_access_key",
        alias = "kinesis.credentials.secret"
    )]
    pub credentials_secret_access_key: Option<String>,
    #[serde(
        rename = "aws.credentials.session_token",
        alias = "kinesis.credentials.session_token"
    )]
    pub session_token: Option<String>,
    #[serde(rename = "aws.credentials.role.arn", alias = "kinesis.assumerole.arn")]
    pub assume_role_arn: Option<String>,
    #[serde(
        rename = "aws.credentials.role.external_id",
        alias = "kinesis.assumerole.external_id"
    )]
    pub assume_role_external_id: Option<String>,
}

impl KinesisCommon {
    pub(crate) async fn build_client(&self) -> anyhow::Result<KinesisClient> {
        let config = AwsAuthProps {
            region: Some(self.stream_region.clone()),
            endpoint: self.endpoint.clone(),
            access_key: self.credentials_access_key.clone(),
            secret_key: self.credentials_secret_access_key.clone(),
            session_token: self.session_token.clone(),
            arn: self.assume_role_arn.clone(),
            external_id: self.assume_role_external_id.clone(),
            profile: Default::default(),
        };
        let aws_config = config.build_config().await?;
        let mut builder = aws_sdk_kinesis::config::Builder::from(&aws_config);
        if let Some(endpoint) = &config.endpoint {
            builder = builder.endpoint_url(endpoint);
        }
        Ok(KinesisClient::from_conf(builder.build()))
    }
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct ClickHouseCommon {
    #[serde(rename = "clickhouse.url")]
    pub url: String,
    #[serde(rename = "clickhouse.user")]
    pub user: String,
    #[serde(rename = "clickhouse.password")]
    pub password: String,
    #[serde(rename = "clickhouse.database")]
    pub database: String,
    #[serde(rename = "clickhouse.table")]
    pub table: String,
    #[serde(rename = "clickhouse.sign")]
    pub sign: Option<String>,
}

impl ClickHouseCommon {
    pub(crate) fn build_client(&self) -> anyhow::Result<ClickHouseClient> {
        let client = ClickHouseClient::default()
            .with_url(&self.url)
            .with_user(&self.user)
            .with_password(&self.password);
        Ok(client)
    }
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct RedisCommon {
    #[serde(rename = "redis.url")]
    pub url: String,
    #[serde(rename = "redis.keyformat")]
    pub key_format: Option<String>,
    #[serde(rename = "redis.valueformat")]
    pub value_format: Option<String>,
}

impl RedisCommon {
    pub(crate) fn build_client(&self) -> anyhow::Result<RedisClient> {
        let client = RedisClient::open(self.url.clone())?;
        Ok(client)
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct UpsertMessage<'a> {
    #[serde(borrow)]
    pub primary_key: Cow<'a, [u8]>,
    #[serde(borrow)]
    pub record: Cow<'a, [u8]>,
}
