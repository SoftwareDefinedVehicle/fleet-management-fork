use log::debug;
use paho_mqtt::{Error, Properties};
use uprotocol_sdk::{
    uprotocol::{
        Data, UAttributes, UCode, UMessage, UMessageType, UPayload, UPayloadFormat, UPriority,
        UStatus, UUri, Uuid,
    },
    uri::serializer::{LongUriSerializer, UriSerializer},
};

const CE_PROPERTY_DATACONTENTTYPE: &str = "datacontenttype";
const CE_PROPERTY_ID: &str = "id";
const CE_PROPERTY_PRIORITY: &str = "priority";
const CE_PROPERTY_SOURCE: &str = "source";
const CE_PROPERTY_SPECVERSION: &str = "specversion";
const CE_PROPERTY_TYPE: &str = "type";

const UATTRIBUTES_COMMSTATUS: &str = "commstatus";
const UATTRIBUTES_PERMISSION_LEVEL: &str = "permission_level";
const UATTRIBUTES_REQUEST_ID: &str = "req_id";
const UATTRIBUTES_SINK: &str = "sink";
const UATTRIBUTES_TTL: &str = "ttl";

pub fn get_mqtt_topic(topic: &UUri) -> String {
    let mqtt_topic = format!(
        "{}/{}/{}/{}/{}/{}",
        topic
            .authority
            .as_ref()
            .and_then(|auth| auth.get_name())
            .unwrap_or(""),
        topic.entity.as_ref().map_or("", |ent| ent.name.as_str()),
        topic
            .entity
            .as_ref()
            .and_then(|ent| ent.version_major)
            .map_or("".to_string(), |v| v.to_string()),
        topic
            .resource
            .as_ref()
            .map_or("".to_string(), |r| r.name.clone()),
        topic.resource.as_ref().map_or("", |r| r.instance()),
        topic.resource.as_ref().map_or("", |r| r.message()),
    );
    debug!(
        "mapped uProtocol topic [{}] to MQTT topic [{}]",
        topic, mqtt_topic
    );
    mqtt_topic
}

pub fn get_umessage(mqtt_message: &paho_mqtt::Message) -> Result<UMessage, UStatus> {
    let attributes = get_attributes(mqtt_message)?;
    let topic = get_topic(mqtt_message, &attributes.r#type())?;
    let payload_format = match mqtt_message.properties().get_string(paho_mqtt::PropertyCode::ContentType) {
        Some(ct) => match ct.as_str() {
            "application/x-protobuf" => UPayloadFormat::UpayloadFormatProtobuf,
            "application/json" => UPayloadFormat::UpayloadFormatJson,
            _ => UPayloadFormat::UpayloadFormatUnspecified,
        },
        None => UPayloadFormat::UpayloadFormatProtobuf,
    };
    let payload = UPayload {
        data: Some(Data::Value(mqtt_message.payload().to_vec())),
        length: None,
        format: payload_format.into(),
    };
    Ok(UMessage {
        source: Some(topic),
        attributes: Some(attributes),
        payload: Some(payload),
    })
}

fn get_topic(msg: &paho_mqtt::Message, message_type: &UMessageType) -> Result<UUri, UStatus> {
    match message_type {
        UMessageType::UmessageTypePublish => {
            if let Some(source) = msg.properties().find_user_property(CE_PROPERTY_SOURCE) {
                LongUriSerializer::deserialize(source).map_err(|_e| {
                    UStatus::fail_with_code(
                        UCode::InvalidArgument,
                        "cannot deserialize topic name from 'source' MQTT message property",
                    )
                })
            } else {
                Err(UStatus::fail_with_code(
                    UCode::FailedPrecondition,
                    "MQTT message does not contain source property",
                ))
            }
        }
        _ => {
            if let Some(sink) = msg.properties().find_user_property(UATTRIBUTES_SINK) {
                LongUriSerializer::deserialize(sink).map_err(|_e| {
                    UStatus::fail_with_code(
                        UCode::InvalidArgument,
                        "cannot deserialize topic name from 'sink' MQTT message property",
                    )
                })
            } else {
                Err(UStatus::fail_with_code(
                    UCode::FailedPrecondition,
                    "MQTT message does not contain sink property",
                ))
            }
        }
    }
}

fn get_attributes(msg: &paho_mqtt::Message) -> Result<UAttributes, UStatus> {
    let message_type = match msg.properties().find_user_property(CE_PROPERTY_TYPE) {
        None => {
            return Err(UStatus::fail_with_code(
                UCode::InvalidArgument,
                format!("message has no '{}' property", CE_PROPERTY_TYPE).as_str(),
            ));
        }
        Some(t) => UMessageType::from(t.as_str()),
    };
    let id = match msg.properties().find_user_property(CE_PROPERTY_ID) {
        None => {
            return Err(UStatus::fail_with_code(
                UCode::InvalidArgument,
                format!("message has no '{}' property", CE_PROPERTY_ID).as_str(),
            ));
        }
        Some(id_string) => match Uuid::try_from(id_string) {
            Err(_e) => {
                return Err(UStatus::fail_with_code(
                    UCode::InvalidArgument,
                    format!(
                        "message '{}' property does not contain a uProtocol UUID",
                        CE_PROPERTY_ID
                    )
                    .as_str(),
                ));
            }
            Ok(uuid) => Some(uuid),
        },
    };
    let priority = match msg.properties().find_user_property(CE_PROPERTY_PRIORITY) {
        None => UPriority::UpriorityCs1,
        Some(s) => {
            let mut prio = String::from("UPROTOCOL_");
            prio.push_str(s.as_str());
            UPriority::from_str_name(&prio).unwrap_or(UPriority::UpriorityCs1)
        }
    };
    let commstatus = match msg.properties().find_user_property(UATTRIBUTES_COMMSTATUS) {
        Some(status) => match status.parse::<i32>() {
            Ok(s) => Some(s),
            Err(_e) => {
                return Err(UStatus::fail_with_code(
                    UCode::InvalidArgument,
                    format!("message '{}' is not an integer", UATTRIBUTES_COMMSTATUS).as_str(),
                ));
            }
        },
        None => None,
    };
    let reqid = match msg.properties().find_user_property(UATTRIBUTES_REQUEST_ID) {
        None => None,
        Some(id_string) => match Uuid::try_from(id_string) {
            Err(_e) => {
                return Err(UStatus::fail_with_code(
                    UCode::InvalidArgument,
                    format!(
                        "message '{}' property doe not contain a UUID",
                        UATTRIBUTES_REQUEST_ID
                    )
                    .as_str(),
                ));
            }
            Ok(uuid) => Some(uuid),
        },
    };
    let sink = match msg.properties().find_user_property(UATTRIBUTES_SINK) {
        None => None,
        Some(s) => match LongUriSerializer::deserialize(s) {
            Ok(uuri) => Some(uuri),
            Err(_e) => {
                return Err(UStatus::fail_with_code(
                    UCode::InvalidArgument,
                    format!("message '{}' is not a valid UUri", UATTRIBUTES_SINK).as_str(),
                ));
            }
        },
    };
    let ttl = match msg.properties().find_user_property(UATTRIBUTES_TTL) {
        None => None,
        Some(ttl) => match ttl.parse::<i32>() {
            Err(_e) => {
                return Err(UStatus::fail_with_code(
                    UCode::InvalidArgument,
                    format!("message '{}' is not an integer", UATTRIBUTES_TTL).as_str(),
                ));
            }
            Ok(v) => Some(v),
        },
    };
    let permission_level = match msg
        .properties()
        .find_user_property(UATTRIBUTES_PERMISSION_LEVEL)
    {
        None => None,
        Some(level) => match level.parse::<i32>() {
            Err(_e) => {
                return Err(UStatus::fail_with_code(
                    UCode::InvalidArgument,
                    format!(
                        "message '{}' is not an integer",
                        UATTRIBUTES_PERMISSION_LEVEL
                    )
                    .as_str(),
                ));
            }
            Ok(v) => Some(v),
        },
    };
    Ok(UAttributes {
        commstatus,
        id,
        permission_level,
        priority: priority.into(),
        reqid,
        sink,
        token: None,
        ttl,
        r#type: message_type.into(),
    })
}

pub fn get_content_type(payload_format: &UPayloadFormat) -> &'static str {
    match payload_format {
        UPayloadFormat::UpayloadFormatProtobuf => "application/x-protobuf",
        UPayloadFormat::UpayloadFormatJson => "application/json",
        _ => "application/octet-stream",
    }
}

pub fn get_mqtt_properties(
    attributes: &UAttributes,
    topic: &UUri,
    payload_format: Option<UPayloadFormat>,
) -> Result<Properties, Error> {
    let mut props = Properties::new();
    props.push_string_pair(
        paho_mqtt::PropertyCode::UserProperty,
        CE_PROPERTY_SPECVERSION,
        "1.0",
    )?;
    if let Some(id) = &attributes.id {
        props.push_string_pair(
            paho_mqtt::PropertyCode::UserProperty,
            CE_PROPERTY_ID,
            id.to_hyphenated_string().as_str(),
        )?;
    }
    match LongUriSerializer::serialize(topic) {
        Err(_e) => {
            return Err(paho_mqtt::Error::Conversion);
        }
        Ok(serialized_topic_uri) => match attributes.r#type() {
            UMessageType::UmessageTypePublish => props.push_string_pair(
                paho_mqtt::PropertyCode::UserProperty,
                CE_PROPERTY_SOURCE,
                serialized_topic_uri.as_str(),
            )?,
            _ => props.push_string_pair(
                paho_mqtt::PropertyCode::UserProperty,
                UATTRIBUTES_SINK,
                serialized_topic_uri.as_str(),
            )?,
        },
    };
    props.push_string_pair(
        paho_mqtt::PropertyCode::UserProperty,
        CE_PROPERTY_TYPE,
        attributes.r#type().to_string().as_str(),
    )?;
    if let Some(pf) = payload_format {
        let content_type = get_content_type(&pf);
        props.push_string_pair(
            paho_mqtt::PropertyCode::UserProperty,
            CE_PROPERTY_DATACONTENTTYPE,
            content_type,
        )?;
        props.push_string(paho_mqtt::PropertyCode::ContentType, content_type)?;
    }
    if let Ok(prio) = UPriority::try_from(attributes.priority) {
        props.push_string_pair(
            paho_mqtt::PropertyCode::UserProperty,
            CE_PROPERTY_PRIORITY,
            prio.as_str_name().rsplit_once('_').unwrap().1,
        )?;
    }
    if let Some(commstatus) = attributes.commstatus {
        props.push_string_pair(
            paho_mqtt::PropertyCode::UserProperty,
            UATTRIBUTES_COMMSTATUS,
            &commstatus.to_string(),
        )?;
    }
    if let Some(ttl) = attributes.ttl {
        props.push_string_pair(
            paho_mqtt::PropertyCode::UserProperty,
            UATTRIBUTES_TTL,
            &ttl.to_string(),
        )?;
    }
    Ok(props)
}
