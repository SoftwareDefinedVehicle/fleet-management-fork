use clap::{Arg, Command};
use json::JsonValue;
use log::{debug, info, warn};

use mqtt_transport::{
    mqtt5_transport::{self, Mqtt5Transport},
    mqtt_connection::MqttClientOptions,
};
use paho_mqtt::MessageBuilder;
use prost::Message;
use std::sync::mpsc;
use uprotocol_sdk::{
    transport::datamodel::UTransport,
    uprotocol::{
        core::usubscription::v3::{SubscriberInfo, Update},
        Data, UCode, UMessage, UPayload, UStatus, UUri,
    },
    uri::serializer::{LongUriSerializer, UriSerializer},
};

// const TOPIC_FILTER: &str = "+/+/+/uplink/telemetry/+";
const TOPIC_SUBSCRIPTION_NOTIFICATIONS: &str = "/core.usubscription/3/subscriptions#Update";
// const FMS_FORWARDER_PUBLISH_UURI: &str = "/fms-forwarder/1/uplink.telemetry#VehicleStatus";

fn parse_uri_from_json(json: &JsonValue) -> UUri {
    let mut topic = String::default();
    if let Some(v) = json["authority"]["name"].as_str() {
        topic.push_str("//");
        topic.push_str(v);
    }
    topic.push('/');
    if let Some(v) = json["entity"]["name"].as_str() {
        topic.push_str(v);
    }
    topic.push('/');
    if let Some(v) = json["entity"]["versionMajor"].as_number() {
        topic.push_str(v.to_string().as_str());
    }
    topic.push('/');
    if let Some(v) = json["resource"]["name"].as_str() {
        topic.push_str(v);
        if let Some(v) = json["resource"]["instance"].as_str() {
            topic.push('.');
            topic.push_str(v);
        }
    }
    if let Some(v) = json["resource"]["message"].as_str() {
        topic.push('#');
        topic.push_str(v);
    }
    UUri::from(topic.as_str())
}

fn get_update_from_payload(payload: &UPayload) -> Result<Update, UStatus> {
    if let Some(Data::Value(data)) = payload.data.as_ref() {
        match payload.format() {
            uprotocol_sdk::uprotocol::UPayloadFormat::UpayloadFormatProtobuf => {
                Update::decode(data.as_ref()).map_err(|_e| {
                    UStatus::fail_with_code(UCode::Internal, "failed to decode Update protobuf")
                })
            }
            uprotocol_sdk::uprotocol::UPayloadFormat::UpayloadFormatJson => {
                match std::str::from_utf8(data.as_ref()) {
                    Err(_e) => Err(UStatus::fail(
                        "MQTT message payload does not contain UTF-8 encoded text",
                    )),
                    Ok(utf8_text) => {
                        debug!(
                            "trying to parse MQTT message payload into Update message: {}",
                            utf8_text
                        );
                        if let Ok(parsed) = json::parse(utf8_text) {
                            Ok(Update {
                                topic: Some(parse_uri_from_json(&parsed["topic"])),
                                subscriber: Some(SubscriberInfo {
                                    uri: Some(parse_uri_from_json(&parsed["subscriber"]["uri"])),
                                    ..Default::default()
                                }),
                                ..Default::default()
                            })
                        } else {
                            debug!("cannot deserialize MQTT message payload");
                            Err(UStatus::fail(
                                "MQTT message payload does not contain JSON encoded Update message",
                            ))
                        }
                    }
                }
            }
            _ => Err(UStatus::fail_with_code(
                UCode::Unimplemented,
                "unsupported payload format",
            )),
        }
    } else {
        Err(UStatus::fail("payload does not contain data"))
    }
}

async fn process_subscription_update(
    utransport: &Mqtt5Transport,
    msg: UMessage,
    uplink_sender: mpsc::Sender<UMessage>,
    vin: &str,
) {
    if let Some(payload) = msg.payload {
        // if let Ok(update) = deserialize_json::<Update>(payload) {
        match get_update_from_payload(&payload) {
            Ok(update) => {
                if let Some(subscriber_uri) = update.subscriber.and_then(|s| s.uri) {
                    let subscribed_topic = update.topic.unwrap();
                    info!(
                        "processing subscription update [subscriber: {}, topic: {}]",
                        subscriber_uri, subscribed_topic
                    );
                    if let (Some(subscriber_authority), Some(subscribed_topic_authority)) = (
                        subscriber_uri.authority.as_ref().and_then(|a| a.get_name()),
                        subscribed_topic
                            .authority
                            .as_ref()
                            .and_then(|a| a.get_name()),
                    ) {
                        if subscriber_authority.starts_with("hono.")
                            && subscribed_topic_authority.eq(vin)
                        {
                            let subscribed_topic_name = subscribed_topic.to_string();
                            if let Err(register_error) = utransport.register_listener(subscribed_topic.clone(), Box::new(move |msg| {
                                match msg {
                                    Ok(message) => {
                                        if let Err(send_error) = uplink_sender.send(message) {
                                            warn!("failed to forward message published on {} to uplink: {}", subscribed_topic_name, send_error);
                                        }
                                    },
                                    Err(s) => {
                                        warn!("received (unexpected) UStatus instead of UMessage on topic [{}]: {}", subscribed_topic_name, s.message());
                                    },
                                }
                            })).await {
                                warn!("failed to register listener for messages published to {}: {}", subscribed_topic, register_error.message());
                            }
                        }
                    };
                }
            }
            Err(e) => debug!("failed to deserialize Update message: {}", e.message()),
        }
    } else {
        debug!("ignoring MQTT message without payload");
    }
}

struct HonoStreamer {
    vin: String,
}

impl HonoStreamer {
    async fn register_subscription_update_listener(
        &self,
        utransport: Mqtt5Transport,
        uplink: mpsc::Sender<UMessage>,
    ) -> Result<String, UStatus> {
        let topic =
            LongUriSerializer::deserialize(TOPIC_SUBSCRIPTION_NOTIFICATIONS.to_string()).unwrap();
        let (subscription_update_sender, subscription_update_receiver) =
            mpsc::channel::<UMessage>();
        let subscription_update_listener =
            Box::new(move |msg: Result<UMessage, UStatus>| match msg {
                Ok(message) => {
                    if let Err(e) = subscription_update_sender.send(message) {
                        info!("failed to send subscription update: {}", e);
                    }
                }
                Err(s) => {
                    warn!(
                        "received unexpected error instead of subscription update: {}",
                        s.message()
                    );
                }
            });
        let sid = utransport
            .register_listener(topic, subscription_update_listener)
            .await?;
        let vehicle_id = self.vin.clone();
        tokio::spawn(async move {
            while let Ok(subscription_update_message) = subscription_update_receiver.recv() {
                info!("processing subscription update notification");
                let uplink_sender = uplink.clone();
                process_subscription_update(
                    &utransport,
                    subscription_update_message,
                    uplink_sender,
                    &vehicle_id,
                )
                .await;
            }
            info!("listener for subscription updates has stopped");
        });
        Ok(sid)
    }

    async fn run(
        &self,
        mqtt_utransport_options: MqttClientOptions,
        hono_client_options: MqttClientOptions,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let utransport = Mqtt5Transport::new(mqtt_utransport_options).await?;
        let hono_client = hono_client_options.connect_v3().await?;

        let (uplink_sender, uplink_recv) = std::sync::mpsc::channel::<UMessage>();
        let _sid = self
            .register_subscription_update_listener(utransport, uplink_sender)
            .await
            .map_err(|s| Box::from(s.message()) as Box<dyn std::error::Error>)?;

        while let Ok(message) = uplink_recv.recv() {
            debug!(
                "processing message [type: {}, source: {}]",
                message
                    .attributes
                    .map_or("unknown", |attr| attr.r#type().as_str_name()),
                message
                    .source
                    .map_or("unknown".to_string(), |uri| uri.to_string()),
            );
            let mut hono_mqtt_topic = String::from("telemetry");
            if let Some(payload) = &message.payload {
                hono_mqtt_topic.push_str("/?content-type=");
                let ct = urlencoding::encode(mqtt5_transport::uprotocol::get_content_type(
                    &payload.format(),
                ))
                .to_string();
                hono_mqtt_topic.push_str(ct.as_ref());
            }
            let mut builder = MessageBuilder::new().topic(&hono_mqtt_topic).qos(0);

            if let Some(Data::Value(data)) = message.payload.and_then(|p| p.data) {
                builder = builder.payload(data);
            }

            match hono_client.publish(builder.finalize()).await {
                Ok(_t) => {
                    debug!(
                        "successfully published message to MQTT endpoint [uri: {}, topic: {}]",
                        hono_client.server_uri(),
                        hono_mqtt_topic
                    );
                }
                Err(e) => {
                    warn!(
                        "error publishing message to MQTT endpoint [uri: {}, topic: {}]: {}",
                        hono_client.server_uri(),
                        hono_mqtt_topic,
                        e
                    );
                }
            }
        }
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    let version = option_env!("VERGEN_GIT_SEMVER_LIGHTWEIGHT")
        .unwrap_or(option_env!("VERGEN_GIT_SHA").unwrap_or("unknown"));
    let mut hono_client_options = MqttClientOptions::using_prefix("hono");
    let mut mqtt_utransport_options = MqttClientOptions::using_prefix("up");
    let mut parser = Command::new("hono-streamer-mqtt")
        // .arg_required_else_help(true)
        .version(version)
        .about("Forwards uProtocol messages received via MQTT to Eclipse Hono's MQTT adapter")
        .arg(
            Arg::new("vin")
                .value_parser(clap::builder::NonEmptyStringValueParser::new())
                .help("This vehicle's VIN")
                .value_name("IDENTIFIER")
                .required(false)
                .env("VIN")
                .default_value("YV2E4C3A5VB180691"),
        );
    parser = mqtt_utransport_options.add_command_line_args(parser);
    parser = hono_client_options.add_command_line_args(parser);

    let args = parser.get_matches();
    mqtt_utransport_options.parse_args(&args);
    hono_client_options.parse_args(&args);
    info!("starting Hono uProtocol Streamer");
    let streamer = HonoStreamer {
        vin: args.get_one::<String>("vin").unwrap().clone(),
    };
    streamer
        .run(mqtt_utransport_options, hono_client_options)
        .await
}
