use std::{
    collections::{BTreeSet, HashMap},
    sync::{Arc, Mutex},
    thread,
    time::Duration,
};

use async_trait::async_trait;
use clap::{Arg, ArgMatches, Command};
use log::{debug, error, info, trace, warn};
use paho_mqtt::{
    AsyncClient, ConnectOptionsBuilder, CreateOptionsBuilder, Error, MessageBuilder, PropertyCode,
};
use uprotocol_sdk::{
    transport::datamodel::UTransport,
    uprotocol::{Data, UAttributes, UCode, UEntity, UMessage, UPayload, UStatus, UUri},
};

pub mod uprotocol;

const PARAM_CA_PATH: &str = "up-ca-path";
const PARAM_MQTT_URI: &str = "up-mqtt-uri";
const PARAM_MQTT_USERNAME: &str = "up-mqtt-username";
const PARAM_MQTT_PASSWORD: &str = "up-mqtt-password";
const PARAM_TRUST_STORE_PATH: &str = "up-trust-store-path";

const QOS: i32 = 1;

type DataRef = Arc<Mutex<MqttClientState>>;
type MessageHandlerRef = Box<dyn Fn(paho_mqtt::Message) + Send + 'static>;

struct MqttClientState {
    // mapping of MQTT 5 Subscription IDs to corresponding message handlers
    subscribed_topics: HashMap<i32, TopicListener>,
    // contains unused subscription IDs
    unused_subscription_ids: BTreeSet<i32>,
}

impl MqttClientState {
    fn next_subscription_id(&mut self) -> i32 {
        let sid = self.unused_subscription_ids.pop_first().unwrap();
        let next_sid = sid + 1;
        if !self.subscribed_topics.contains_key(&next_sid) {
            self.unused_subscription_ids.insert(next_sid);
        }
        sid
    }

    pub fn add_subscribed_topic(
        &mut self,
        topic_filter: String,
        message_handler: MessageHandlerRef,
    ) -> i32 {
        let subscription_id = self.next_subscription_id();
        let listener = TopicListener {
            topic_filter,
            message_handler,
        };
        self.subscribed_topics.insert(subscription_id, listener);
        subscription_id
    }

    pub fn remove_subscription(&mut self, subscription_id: i32) {
        self.subscribed_topics.remove(&subscription_id);
        self.unused_subscription_ids.insert(subscription_id);
    }

    pub fn get_listener(&self, subscription_id: &i32) -> Option<&TopicListener> {
        self.subscribed_topics.get(subscription_id)
    }

    pub fn subscribed_topics(&self) -> HashMap<i32, String> {
        let mut r = HashMap::new();
        for (k, v) in &self.subscribed_topics {
            r.insert(*k, v.topic_filter.clone());
        }
        r
    }
}

struct TopicListener {
    // the MQTT topic filter used to create the subscription
    topic_filter: String,
    // the handler to invoke for each MQTT message matching the filter
    message_handler: MessageHandlerRef,
}

impl TopicListener {
    pub fn handle_message(&self, message: paho_mqtt::Message) {
        (self.message_handler)(message)
    }
}

pub fn add_command_line_args(command: Command) -> Command {
    command
        .arg(
            Arg::new(PARAM_MQTT_URI)
                .value_parser(clap::builder::NonEmptyStringValueParser::new())
                .long(PARAM_MQTT_URI)
                .help("The URI of the MQTT broker to use as transport.")
                .value_name("URI")
                .required(false)
                .default_value("mqtt://localhost:1883")
                .env("UP_MQTT_URI"),
        )
        .arg(
            Arg::new(PARAM_MQTT_USERNAME)
                .value_parser(clap::builder::NonEmptyStringValueParser::new())
                .long(PARAM_MQTT_USERNAME)
                .help("The username for authenticating to the MQTT broker.")
                .value_name("USERNAME")
                .required(false)
                .env("UP_MQTT_USERNAME"),
        )
        .arg(
            Arg::new(PARAM_MQTT_PASSWORD)
                .value_parser(clap::builder::NonEmptyStringValueParser::new())
                .long(PARAM_MQTT_PASSWORD)
                .help("The password for authenticating to the MQTT broker.")
                .value_name("PWD")
                .required(false)
                .env("UP_MQTT_PASSWORD"),
        )
        .arg(
            Arg::new(PARAM_CA_PATH)
                .value_parser(clap::builder::NonEmptyStringValueParser::new())
                .long(PARAM_CA_PATH)
                .help("The path to a folder that contains PEM files for trusted certificate authorities.")
                .value_name("PATH")
                .required(false)
                .env("UP_CA_PATH"),
        )
        .arg(
            Arg::new(PARAM_TRUST_STORE_PATH)
                .value_parser(clap::builder::NonEmptyStringValueParser::new())
                .long(PARAM_TRUST_STORE_PATH)
                .help("The path to a file that contains PEM encoded trusted certificates.")
                .value_name("PATH")
                .required(false)
                .env("UP_TRUST_STORE_PATH"),
        )
}

pub struct Mqtt5Transport {
    mqtt_client: AsyncClient,
    pub uri: String,
}

impl Mqtt5Transport {
    fn on_connect_success(cli: &AsyncClient, _msgid: u16) {
        info!("successfully connected to MQTT broker");
        let data = cli.user_data().unwrap();

        if let Some(data_ref) = data.downcast_ref::<DataRef>() {
            let mqtt_client_state = data_ref.lock().unwrap();
            for (id, topic_filter) in mqtt_client_state.subscribed_topics() {
                let props = paho_mqtt::properties![
                    PropertyCode::SubscriptionIdentifier => id
                ];
                // Subscribe to the desired topic(s).
                if let Err(e) = cli
                    .subscribe_with_options(&topic_filter, QOS, None, props)
                    .wait_for(Duration::from_secs(3))
                {
                    error!(
                        "failed to (re-)subscribe to topic [{}]: {}",
                        topic_filter, e
                    );
                } else {
                    debug!(
                        "successfully (re-)subscribed to MQTT topic [{}]",
                        topic_filter
                    );
                }
            }
        } else {
            warn!("can not access client state, will not reestablish subscriptions to topics...");
        }
    }

    fn on_connect_failure(client: &AsyncClient, _msgid: u16, rc: i32) {
        warn!(
            "attempt to connect to MQTT broker failed with error code {}, retrying ...",
            rc
        );
        thread::sleep(Duration::from_secs(3));
        client.reconnect_with_callbacks(Self::on_connect_success, Self::on_connect_failure);
    }

    fn on_message_received(client: &AsyncClient, msg: Option<paho_mqtt::Message>) {
        if msg.is_none() {
            return;
        }
        let mqtt_message = msg.unwrap();
        let mqtt_topic = mqtt_message.topic().to_owned();
        debug!("received MQTT message [topic: {}]", mqtt_topic);

        let user_data = client.user_data().unwrap();
        match user_data.downcast_ref::<DataRef>() {
            Some(data_ref) => {
                let client_state = data_ref.lock().unwrap();
                for subscription_id in mqtt_message
                    .properties()
                    .iter(paho_mqtt::PropertyCode::SubscriptionIdentifier)
                {
                    let sid = subscription_id.get_int().unwrap();
                    trace!("using subscription ID {} to determine message handler", sid);
                    if let Some(topic_listener) = client_state.get_listener(&sid) {
                        topic_listener.handle_message(mqtt_message.clone());
                    } else {
                        debug!("no listener registered for MQTT subscription ID [{}]", sid);
                    }
                }
            }
            None => {
                warn!("can not access client state, ignoring MQTT message ...");
            }
        };
    }

    pub fn get_v5_connect_options() -> Result<paho_mqtt::ConnectOptions, Box<dyn std::error::Error>>
    {
        let mut connect_options_builder = ConnectOptionsBuilder::new_v5();
        connect_options_builder.connect_timeout(Duration::from_secs(5));
        connect_options_builder.clean_start(true);
        connect_options_builder.keep_alive_interval(Duration::from_secs(10));
        connect_options_builder.max_inflight(10);
        Ok(connect_options_builder.finalize())
    }

    /// Creates a new transport for an MQTT 5 broker.
    pub async fn new(args: &ArgMatches) -> Result<Self, Box<dyn std::error::Error>> {
        let mqtt_uri = args.get_one::<String>(PARAM_MQTT_URI).unwrap().to_owned();
        let client_state = MqttClientState {
            subscribed_topics: HashMap::new(),
            unused_subscription_ids: BTreeSet::from([1]),
        };
        info!("connecting to MQTT broker at {}", mqtt_uri);
        let connect_options = Self::get_v5_connect_options()?;
        match CreateOptionsBuilder::new()
            .server_uri(&mqtt_uri)
            .max_buffered_messages(50)
            .send_while_disconnected(true)
            .delete_oldest_messages(true)
            .user_data(Box::new(Arc::new(Mutex::new(client_state))))
            .create_client()
        {
            Err(e) => {
                error!("failed to create MQTT client: {}", e);
                Err(Box::new(e))
            }
            Ok(client) => {
                client.set_message_callback(Self::on_message_received);
                client
                    .connect_with_callbacks(
                        connect_options,
                        Self::on_connect_success,
                        Self::on_connect_failure,
                    )
                    .await
                    .map(|_r| {
                        Ok(Self {
                            mqtt_client: client,
                            uri: mqtt_uri,
                        })
                    })?
            }
        }
    }

    fn add_subscribed_topic(
        &self,
        mqtt_topic: String,
        listener: Box<dyn Fn(paho_mqtt::Message) + Send + 'static>,
    ) -> Result<i32, Box<dyn std::error::Error>> {
        let data = self.mqtt_client.user_data().unwrap();
        match data.downcast_ref::<DataRef>() {
            Some(data_ref) => {
                let mut client_state = data_ref.lock().unwrap();
                let sid = client_state.add_subscribed_topic(mqtt_topic, listener);
                Ok(sid)
            }
            None => Err(Box::from("can not access client state")),
        }
    }

    pub async fn subscribe<T>(
        &self,
        mqtt_topic: &str,
        listener: T,
    ) -> Result<usize, Box<dyn std::error::Error>>
    where
        T: Fn(paho_mqtt::Message) + Send + 'static,
    {
        let sid = self.add_subscribed_topic(mqtt_topic.to_string(), Box::new(listener))?;
        debug!(
            "subscribing to MQTT topic filter [{}] using subscription ID: {}",
            mqtt_topic, sid
        );
        let props = paho_mqtt::properties![
            PropertyCode::SubscriptionIdentifier => sid
        ];
        self.mqtt_client
            .subscribe_with_options(mqtt_topic, QOS, None, props)
            .await
            .map(|_r| sid as usize)
            .map_err(|e| {
                let data = self.mqtt_client.user_data().unwrap();
                if let Some(data_ref) = data.downcast_ref::<DataRef>() {
                    let mut client_state = data_ref.lock().unwrap();
                    client_state.remove_subscription(sid);
                    Box::from(e)
                } else {
                    Box::from(Error::General("can not access client state"))
                }
            })
    }

    fn get_topic_filter(&self, subscription_id: i32) -> Result<String, Box<dyn std::error::Error>> {
        let data = self.mqtt_client.user_data().unwrap();
        match data.downcast_ref::<DataRef>() {
            Some(data_ref) => {
                let client_state = data_ref.lock().unwrap();
                client_state
                    .get_listener(&subscription_id)
                    .map_or(Err(Box::from("no such subscription ID")), |tl| {
                        Ok(tl.topic_filter.clone())
                    })
            }
            None => Err(Box::from(Error::General("can not access client state"))),
        }
    }

    pub async fn unsubscribe(
        &self,
        subscription_id: i32,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let topic_filter = self.get_topic_filter(subscription_id)?;
        debug!("unsubscribing from MQTT topic filter [{}]", topic_filter);
        match self.mqtt_client.unsubscribe(&topic_filter).await {
            Ok(_r) => {
                let data = self.mqtt_client.user_data().unwrap();
                if let Some(data_ref) = data.downcast_ref::<DataRef>() {
                    let mut client_state = data_ref.lock().unwrap();
                    client_state.remove_subscription(subscription_id);
                    Ok(())
                } else {
                    Err(Box::from(Error::General("can not access client state")))
                }
            }
            Err(e) => Err(Box::from(e)),
        }
    }
}

#[async_trait]
impl UTransport for Mqtt5Transport {
    /// API to register the calling uE with the underlying transport implementation.
    async fn authenticate(&self, _uentity: UEntity) -> Result<(), UStatus> {
        Ok(())
    }

    /// Transmit UPayload to the topic using the attributes defined in UTransportAttributes.
    async fn send(
        &self,
        topic: UUri,
        payload: UPayload,
        attributes: UAttributes,
    ) -> Result<(), UStatus> {
        let mqtt_topic_name = uprotocol::get_mqtt_topic(&topic);
        let mqtt_properties =
            uprotocol::get_mqtt_properties(&attributes, &topic, Some(payload.format())).map_err(
                |_e| {
                    UStatus::fail_with_code(
                        UCode::InvalidArgument,
                        "failed to create MQTT message properties",
                    )
                },
            )?;
        let mut builder = MessageBuilder::new()
            .topic(&mqtt_topic_name)
            .properties(mqtt_properties)
            .qos(1);
        if let Some(Data::Value(data)) = payload.data {
            builder = builder.payload(data);
        }
        let msg = builder.finalize();
        match self.mqtt_client.publish(msg).await {
            Ok(_t) => {
                debug!(
                    "successfully published message to MQTT endpoint [uri: {}, topic: {}]",
                    self.uri, &mqtt_topic_name
                );
                Ok(())
            }
            Err(e) => {
                warn!(
                    "error publishing message to MQTT endpoint [uri: {}, topic: {}]: {}",
                    self.uri, &mqtt_topic_name, e
                );
                Err(UStatus::fail_with_code(
                    UCode::Unavailable,
                    "failed to publish message",
                ))
            }
        }
    }

    /// Register a method that will be called when a message comes in on the specific topic.
    async fn register_listener(
        &self,
        topic: UUri,
        listener: Box<dyn Fn(Result<UMessage, UStatus>) + Send + Sync + 'static>,
    ) -> Result<String, UStatus> {
        let mqtt_topic = uprotocol::get_mqtt_topic(&topic);
        self.subscribe(&mqtt_topic, move |msg| {
            match uprotocol::get_umessage(&msg) {
                Ok(umessage) => listener(Ok(umessage)),
                Err(e) => warn!("failed to create UMessage from MQTT message: {}", e.code),
            }
        })
        .await
        .map_err(|e| UStatus::fail(e.to_string().as_ref()))
        .map(|subscription_id| subscription_id.to_string())
    }

    /// Unregister a method on a topic. Messages arriving on this topic will no longer be processed by this listener.
    async fn unregister_listener(&self, _topic: UUri, sid: &str) -> Result<(), UStatus> {
        let subscription_id = sid.parse::<i32>().map_err(|_e| {
            UStatus::fail_with_code(
                UCode::InvalidArgument,
                "given identifier is not an unsigned integer",
            )
        })?;
        match self.unsubscribe(subscription_id).await {
            Ok(_) => Ok(()),
            Err(_e) => Err(UStatus::fail_with_code(
                UCode::Internal,
                "cannot unregister listener",
            )),
        }
    }
}
