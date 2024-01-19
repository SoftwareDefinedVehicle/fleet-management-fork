// SPDX-FileCopyrightText: 2023 Contributors to the Eclipse Foundation
//
// See the NOTICE file(s) distributed with this work for additional
// information regarding copyright ownership.
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
//
// SPDX-License-Identifier: Apache-2.0

use clap::{builder::Str, Arg, ArgMatches, Command};
use log::{debug, error, info, warn};
use mqtt::{
    AsyncClient, ConnectOptions, ConnectOptionsBuilder, CreateOptionsBuilder, SslOptions,
    SslOptionsBuilder,
};
use paho_mqtt as mqtt;
use std::{
    fmt::Display,
    thread,
    time::Duration,
};

const PARAM_CA_PATH: &str = "ca-path";
const PARAM_DEVICE_CERT: &str = "device-cert";
const PARAM_DEVICE_KEY: &str = "device-key";
const PARAM_ENABLE_HOSTNAME_VERIFICATION: &str = "enable-hostname-verification";
const PARAM_MQTT_CLIENT_ID: &str = "mqtt-client-id";
const PARAM_MQTT_URI: &str = "mqtt-uri";
const PARAM_MQTT_USERNAME: &str = "mqtt-username";
const PARAM_MQTT_PASSWORD: &str = "mqtt-password";
const PARAM_TRUST_STORE_PATH: &str = "trust-store-path";

pub fn on_connect_success(_client: &AsyncClient, _msgid: u16) {
    info!("successfully connected to MQTT endpoint");
}

pub fn on_connect_failure(client: &AsyncClient, _msgid: u16, rc: i32) {
    warn!(
        "attempt to connect to MQTT endpoint failed with error code {}, retrying ...",
        rc
    );
    thread::sleep(Duration::from_secs(3));
    client.reconnect_with_callbacks(on_connect_success, on_connect_failure);
}

pub struct MqttClientOptions {
    param_prefix: Option<String>,
    client_id: String,
    server_uri: String,
    client_username: Option<String>,
    client_password: Option<String>,
    client_cert_path: Option<String>,
    client_key_path: Option<String>,
    client_ca_path: Option<String>,
    client_trust_store_path: Option<String>,
    enable_hostname_verification: bool,
}

impl Default for MqttClientOptions {
    fn default() -> Self {
        MqttClientOptions {
            param_prefix: None,
            client_id: String::from(""),
            server_uri: String::from("mqtt://localhost:1883"),
            client_username: None,
            client_password: None,
            client_cert_path: None,
            client_key_path: None,
            client_ca_path: None,
            client_trust_store_path: None,
            enable_hostname_verification: true,
        }
    }
}

impl Display for MqttClientOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!(
            "MQTT Client Options [prefix: {:?}, server-uri: {}]",
            self.param_prefix,
            self.server_uri
        ))
    }
}

impl MqttClientOptions {
    pub fn new() -> Self {
        MqttClientOptions::default()
    }

    pub fn using_prefix<P: Into<String>>(prefix: P) -> Self {
        MqttClientOptions {
            param_prefix: Some(prefix.into()),
            ..Default::default()
        }
    }

    fn prefixed_param_name(&self, param_name: &str) -> Str {
        self.param_prefix.as_ref().map_or_else(
            || Str::from(param_name.to_string()),
            |p| Str::from(format!("{}-{}", p.to_lowercase(), param_name)),
        )
    }

    fn prefixed_env_variable(&self, param_name: &str) -> Str {
        self.param_prefix.as_ref().map_or_else(
            || Str::from(param_name.to_uppercase()),
            |p| {
                Str::from(format!(
                    "{}_{}",
                    p.to_uppercase(),
                    param_name.to_uppercase()
                ))
            },
        )
    }

    fn get_string_param(&self, param_name: &str, args: &ArgMatches) -> Option<String> {
        let prefixed_param_name = self.prefixed_param_name(param_name);
        args.get_one::<String>(&prefixed_param_name)
            .map(|s| s.to_string())
    }

    /// Adds arguments to an existing command line which can be
    /// used to configure the connection to an MQTT endpoint.
    ///
    /// The following arguments are being added:
    ///
    /// | Long Name                    | Environment Variable         | Default Value |
    /// |------------------------------|------------------------------|---------------|
    /// | mqtt-client-id               | MQTT_CLIENT_ID               | -             |
    /// | mqtt-uri                     | MQTT_URI                     | -             |
    /// | mqtt-username                | MQTT_USERNAME                | -             |
    /// | mqtt-password                | MQTT_PASSWORD                | -             |
    /// | device-cert                  | DEVICE_CERT                  | -             |
    /// | device-key                   | DEVICE_KEY                   | -             |
    /// | ca-path                      | CA_PATH                      | -             |
    /// | trust-store-path             | TRUST_STORE_PATH             | -             |
    /// | enable-hostname-verification | ENABLE_HOSTNAME_VERIFICATION | `true`        |
    ///
    pub fn add_command_line_args(&self, command: Command) -> Command {
        let p_client_id = self.prefixed_param_name(PARAM_MQTT_CLIENT_ID);
        let p_uri = self.prefixed_param_name(PARAM_MQTT_URI);
        let p_username = self.prefixed_param_name(PARAM_MQTT_USERNAME);
        let p_password = self.prefixed_param_name(PARAM_MQTT_PASSWORD);
        let p_client_cert = self.prefixed_param_name(PARAM_DEVICE_CERT);
        let p_client_key = self.prefixed_param_name(PARAM_DEVICE_KEY);
        let p_ca_path = self.prefixed_param_name(PARAM_CA_PATH);
        let p_trust_store_path = self.prefixed_param_name(PARAM_TRUST_STORE_PATH);
        let p_enable_hostname_verification =
            self.prefixed_param_name(PARAM_ENABLE_HOSTNAME_VERIFICATION);
        command
            .arg(
                Arg::new(&p_client_id)
                    .value_parser(clap::builder::NonEmptyStringValueParser::new())
                    .long(&p_client_id)
                    .help("The client identifier to use in the MQTT Connect Packet.")
                    .value_name("ID")
                    .required(false)
                    .env(self.prefixed_env_variable("MQTT_CLIENT_ID")),
            )
            .arg(
                Arg::new(&p_uri)
                    .value_parser(clap::builder::NonEmptyStringValueParser::new())
                    .long(&p_uri)
                    .help("The URI of the MQTT adapter to publish data to.")
                    .value_name("URI")
                    .required(true)
                    .env(self.prefixed_env_variable("MQTT_URI")),
            )
            .arg(
                Arg::new(&p_username)
                    .value_parser(clap::builder::NonEmptyStringValueParser::new())
                    .long(&p_username)
                    .help("The username to use for authenticating to the MQTT endpoint.")
                    .value_name("USERNAME")
                    .required(false)
                    .env(self.prefixed_env_variable("MQTT_USERNAME")),
            )
            .arg(
                Arg::new(&p_password)
                    .value_parser(clap::builder::NonEmptyStringValueParser::new())
                    .long(&p_password)
                    .help("The password to use for authenticating to the MQTT endpoint.")
                    .value_name("PWD")
                    .required(false)
                    .env(self.prefixed_env_variable("MQTT_PASSWORD")),
            )
            .arg(
                Arg::new(&p_client_cert)
                    .value_parser(clap::builder::NonEmptyStringValueParser::new())
                    .long(&p_client_cert)
                    .help("The path to a PEM file containing the X.509 certificate that the device should use for authentication.")
                    .value_name("PATH")
                    .required(false)
                    .env(self.prefixed_env_variable("DEVICE_CERT")),
            )
            .arg(
                Arg::new(&p_client_key)
                    .value_parser(clap::builder::NonEmptyStringValueParser::new())
                    .long(&p_client_key)
                    .help("The path to a PEM file containing the private key that the device should use for authentication.")
                    .value_name("PATH")
                    .required(false)
                    .env(self.prefixed_env_variable("DEVICE_KEY")),
            )
            .arg(
                Arg::new(&p_ca_path)
                    .value_parser(clap::builder::NonEmptyStringValueParser::new())
                    .long(&p_ca_path)
                    .help("The path to a folder that contains PEM files for trusted certificate authorities.")
                    .value_name("PATH")
                    .required(false)
                    .env(self.prefixed_env_variable("CA_PATH")),
            )
            .arg(
                Arg::new(&p_trust_store_path)
                    .value_parser(clap::builder::NonEmptyStringValueParser::new())
                    .long(&p_trust_store_path)
                    .help("The path to a file that contains PEM encoded trusted certificates.")
                    .value_name("PATH")
                    .required(false)
                    .env(self.prefixed_env_variable("TRUST_STORE_PATH")),
            )
            .arg(
                Arg::new(&p_enable_hostname_verification)
                    .value_parser(clap::builder::BoolishValueParser::new())
                    .long(&p_enable_hostname_verification)
                    .help("Indicates whether server certificates should be matched against the hostname/IP address 
                        used by a client to connect to the server.")
                    .value_name("FLAG")
                    .required(false)
                    .default_value("true")
                    .env(self.prefixed_env_variable("ENABLE_HOSTNAME_VERIFICATION")),
            )
    }

    pub fn parse_args(&mut self, args: &ArgMatches) {
        if let Some(id) = self.get_string_param(PARAM_MQTT_CLIENT_ID, args) {
            self.client_id = id;
        }
        if let Some(uri) = self.get_string_param(PARAM_MQTT_URI, args) {
            self.server_uri = uri;
        }
        self.client_username = self.get_string_param(PARAM_MQTT_USERNAME, args);
        self.client_password = self.get_string_param(PARAM_MQTT_PASSWORD, args);
        self.client_cert_path = self.get_string_param(PARAM_DEVICE_CERT, args);
        self.client_key_path = self.get_string_param(PARAM_DEVICE_KEY, args);
        self.client_ca_path = self.get_string_param(PARAM_CA_PATH, args);
        self.client_trust_store_path = self.get_string_param(PARAM_TRUST_STORE_PATH, args);
        if let Some(flag) =
            args.get_one::<bool>(&self.prefixed_param_name(PARAM_ENABLE_HOSTNAME_VERIFICATION))
        {
            self.enable_hostname_verification = *flag;
        }
    }

    pub fn client_id(&self) -> &str {
        self.client_id.as_str()
    }

    pub fn server_uri(&self) -> &str {
        self.server_uri.as_str()
    }

    pub fn username(&self) -> Option<&str> {
        self.client_username.as_deref()
    }

    pub fn password(&self) -> Option<&str> {
        self.client_password.as_deref()
    }

    pub fn ssl_options(&self) -> Result<SslOptions, Box<dyn std::error::Error>> {
        let mut ssl_options_builder = SslOptionsBuilder::new();
        match (
            self.client_cert_path.as_ref(),
            self.client_key_path.as_ref(),
        ) {
            (Some(cert_path), Some(key_path)) => {
                if let Err(e) = ssl_options_builder.key_store(cert_path) {
                    error!("failed to set client certificate for MQTT client: {}", e);
                    return Err(Box::from(e));
                }
                if let Err(e) = ssl_options_builder.private_key(key_path) {
                    error!("failed to set private key for MQTT client: {}", e);
                    return Err(Box::from(e));
                }
                info!("using client certificate for authenticating to MQTT endpoint");
            }
            _ => {
                debug!("no client key material specified");
            }
        }

        if let Some(path) = self.client_ca_path.as_ref() {
            if let Err(e) = ssl_options_builder.ca_path(path) {
                error!("failed to set CA path on MQTT client: {}", e);
                return Err(Box::from(e));
            }
        }
        if let Some(path) = self.client_trust_store_path.as_ref() {
            if let Err(e) = ssl_options_builder.trust_store(path) {
                error!("failed to set trust store path on MQTT client: {}", e);
                return Err(Box::from(e));
            }
        }
        ssl_options_builder.verify(self.enable_hostname_verification);
        Ok(ssl_options_builder.finalize())
    }

    pub fn mqtt3_connect_options(
        &self,
    ) -> Result<paho_mqtt::ConnectOptions, Box<dyn std::error::Error>> {
        let mut connect_options_builder = ConnectOptionsBuilder::new_v3();
        connect_options_builder.connect_timeout(Duration::from_secs(10));
        connect_options_builder
            .automatic_reconnect(Duration::from_secs(1), Duration::from_secs(16));
        connect_options_builder.clean_session(true);
        connect_options_builder.keep_alive_interval(Duration::from_secs(10));
        connect_options_builder.max_inflight(10);
        connect_options_builder.ssl_options(self.ssl_options()?);

        match (self.username(), self.password()) {
            (Some(username), Some(password)) => {
                connect_options_builder.user_name(username);
                connect_options_builder.password(password);
                info!("using username and password for authenticating to MQTT endpoint");
            }
            _ => {
                debug!("no credentials specified");
            }
        }
        Ok(connect_options_builder.finalize())
    }

    pub fn mqtt5_connect_options(
        &self,
    ) -> Result<paho_mqtt::ConnectOptions, Box<dyn std::error::Error>> {
        let options = ConnectOptionsBuilder::new_v5()
            .connect_timeout(Duration::from_secs(5))
            .automatic_reconnect(Duration::from_secs(1), Duration::from_secs(16))
            .clean_start(true)
            .keep_alive_interval(Duration::from_secs(10))
            .max_inflight(10)
            .finalize();
        Ok(options)
    }

    pub fn create_async_client(&self) -> Result<AsyncClient, Box<dyn std::error::Error>> {
        CreateOptionsBuilder::new()
            .server_uri(self.server_uri())
            .max_buffered_messages(50)
            .send_while_disconnected(true)
            .delete_oldest_messages(true)
            .client_id(self.client_id())
            .create_client()
            .map_err(Box::from)
    }

    pub async fn connect(
        &self,
        connect_options: ConnectOptions,
    ) -> Result<AsyncClient, Box<dyn std::error::Error>> {
        match self.create_async_client() {
            Err(e) => {
                error!("failed to create MQTT client: {}", e);
                Err(e)
            }
            Ok(client) => {
                info!("connecting to MQTT endpoint at {}", client.server_uri());
                client
                    .connect_with_callbacks(
                        connect_options,
                        crate::mqtt_connection::on_connect_success,
                        crate::mqtt_connection::on_connect_failure,
                    )
                    .await
                    .map(|_response| client)
                    .map_err(Box::from)
                // Ok(client)
            }
        }
    }

    /// Creates a new connection to an MQTT 3.1.1 endpoint.
    ///
    /// Expects to find parameters as defined by [`add_command_line_args`] in the passed
    /// in *args*.
    ///
    /// The connection returned is configured to keep trying to (re-)connect to the configured
    /// MQTT endpoint.
    pub async fn connect_v3(&self) -> Result<AsyncClient, Box<dyn std::error::Error>> {
        let connect_options = self.mqtt3_connect_options()?;
        self.connect(connect_options).await
    }

    pub async fn connect_v5(&self) -> Result<AsyncClient, Box<dyn std::error::Error>> {
        let connect_options = self.mqtt5_connect_options()?;
        self.connect(connect_options).await
    }
}

#[cfg(test)]
mod tests {
    use crate::mqtt_connection::PARAM_MQTT_URI;

    use super::MqttClientOptions;

    #[test]
    fn test_prefixed_env_variable_uses_prefix() {
        let options = MqttClientOptions::using_prefix("up");
        assert_eq!(options.prefixed_env_variable("MQTT_URI"), "UP_MQTT_URI");
    }

    #[test]
    fn test_prefixed_param_name_uses_prefix() {
        let options = MqttClientOptions::using_prefix("up");
        assert_eq!(options.prefixed_param_name(PARAM_MQTT_URI), "up-mqtt-uri");
    }

    #[test]
    fn test_command_line_requires_uri_arg() {
        let options = super::MqttClientOptions::new();
        let command = options.add_command_line_args(clap::Command::new("mqtt"));
        let matches = command.try_get_matches_from(vec!["mqtt"]);
        assert!(matches.is_err_and(|e| e.kind() == clap::error::ErrorKind::MissingRequiredArgument));
    }

    #[test]
    fn test_command_line_uses_defaults() {
        let mut options = super::MqttClientOptions::new();
        let command = options.add_command_line_args(clap::Command::new("mqtt"));
        let matches =
            command.get_matches_from(vec!["mqtt", "--mqtt-uri", "mqtts://non-existing.host.io"]);
        options.parse_args(&matches);

        assert_eq!(options.server_uri(), "mqtts://non-existing.host.io");
        assert_eq!(options.client_id(), "");
        assert!(options.username().is_none());
        assert!(options.password().is_none());
        assert!(options.client_cert_path.is_none());
        assert!(options.client_key_path.is_none());
        assert!(options.client_ca_path.is_none());
        assert!(options.enable_hostname_verification);
    }
}
