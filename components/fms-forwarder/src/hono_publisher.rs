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

use async_trait::async_trait;
use clap::{ArgMatches, Command};
use fms_proto::fms::VehicleStatus;
use log::{debug, warn};
use protobuf::Message;
use uprotocol_sdk::{
    transport::{builder::UAttributesBuilder, datamodel::UTransport},
    uprotocol::{Data, UPayload, UPayloadFormat, UPriority, UUri},
};

use mqtt_transport::mqtt5_transport::{self, Mqtt5Transport};

use crate::status_publishing::StatusPublisher;

const PUBLISH_TOPIC_TEMPLATE: &str = "/fms-forwarder/1/uplink.telemetry#VehicleStatus";

/// Adds arguments to an existing command line which can be
/// used to configure the connection to a Hono MQTT protocol adapter.
///
/// See [`mqtt5_transport::add_command_line_args`]
///
pub fn add_command_line_args(command: Command) -> Command {
    mqtt5_transport::add_command_line_args(command)
}
pub struct HonoPublisher {
    transport: Mqtt5Transport,
}

impl HonoPublisher {
    /// Creates a new publisher.
    ///
    /// Determines the parameters necessary for creating the publisher from values specified on
    /// the command line or via environment variables as defined by [`add_command_line_args`].
    ///
    /// The publisher returned is configured to keep trying to (re-)connect to the configured
    /// MQTT endpoint using a client certificate of username/password credentials.
    pub async fn new(args: &ArgMatches) -> Result<Self, Box<dyn std::error::Error>> {
        Mqtt5Transport::new(args)
            .await
            .map(|transport| HonoPublisher { transport })
    }
}

#[async_trait]
impl StatusPublisher for HonoPublisher {
    async fn publish_vehicle_status(&self, vehicle_status: &VehicleStatus) {
        // match Any::pack(vehicle_status).and_then(|any| any.write_to_bytes()) {
        match vehicle_status.write_to_bytes() {
            Ok(payload) => {
                let upayload = UPayload {
                    data: Some(Data::Value(payload)),
                    length: None,
                    format: UPayloadFormat::UpayloadFormatProtobuf.into(),
                };
                let attribs = UAttributesBuilder::publish(UPriority::UpriorityCs1).build();
                let source_topic = format!("//{}{}", vehicle_status.vin, PUBLISH_TOPIC_TEMPLATE);
                let topic = UUri::from(source_topic.as_str());
                match self.transport.send(topic, upayload, attribs).await {
                    Ok(_) => debug!(
                        "successfully published vehicle status [topic: {}]",
                        source_topic,
                    ),
                    Err(e) => {
                        warn!(
                            "error publishing vehicle status [topic: {}]: {}",
                            source_topic,
                            e.message()
                        );
                    }
                };
            }
            Err(e) => warn!(
                "error serializing vehicle status to protobuf message: {}",
                e
            ),
        }
    }
}
