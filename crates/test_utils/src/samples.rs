use {
    crate::helpers::create_client_key,
    cerberus::project::{ProjectData, Quota},
    relay_rpc::{
        domain::{ClientId, DecodedProjectId, MessageId, ProjectId, Topic},
        rpc::{Params, Publish, Request},
    },
    relay_storage::{
        keys::{
            ClientProjectRoutingKey,
            DataTag,
            FromBytes,
            LegacyKey,
            MailboxKey,
            PositionedKey,
            RoutingTableKey,
            ToBytes,
        },
        mailbox::{MailboxMessage, NotificationDetails},
        routing_table::RoutingTableEntry,
        util::{deserialize, serialize},
        ClusterKey,
        Serializable,
        StorageError,
        StorageResult,
    },
    serde::{Deserialize, Serialize},
    std::{
        fmt::{Display, Formatter},
        net::{IpAddr, Ipv4Addr, SocketAddr},
    },
};

#[derive(Debug, Clone, Hash, Serialize, Deserialize, PartialEq, Eq)]
pub struct TestData(pub Vec<u8>);

impl TestData {
    pub fn new(data: impl AsRef<[u8]>) -> Self {
        Self(data.as_ref().into())
    }
}

impl Display for TestData {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(self, f)
    }
}

impl LegacyKey for TestData {}

impl ClusterKey for TestData {
    fn data_tag(&self) -> DataTag {
        DataTag::Unknown
    }
}

impl ToBytes for TestData {
    fn to_bytes(&self) -> StorageResult<Vec<u8>> {
        Ok(self.0.clone())
    }
}

impl FromBytes for TestData {
    fn from_bytes(bytes: &[u8]) -> StorageResult<Self> {
        Ok(Self(bytes.into()))
    }
}

// Key prefix similar to [`relay_storage::keys::KeyPrefix`]. The value is chosen
// arbitrarily to not collide with the domain keys.
const BASE_KEY_PREFIX: u8 = 128;

#[derive(Clone, PartialEq, Eq, Debug, Hash, Serialize, Deserialize)]
pub struct ProjectDataCacheKey(pub ProjectId);

impl Display for ProjectDataCacheKey {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "project-data-v3/{}", self.0)
    }
}

impl LegacyKey for ProjectDataCacheKey {}

impl ClusterKey for ProjectDataCacheKey {
    fn data_tag(&self) -> DataTag {
        DataTag::Unknown
    }
}

impl ToBytes for ProjectDataCacheKey {
    fn to_bytes(&self) -> StorageResult<Vec<u8>> {
        let data = self.0.decode().map_err(|_| StorageError::Serialize)?;
        let data = data.as_ref();
        let mut result = Vec::with_capacity(1 + data.len());
        result.push(BASE_KEY_PREFIX + 1);
        result.extend_from_slice(data);
        Ok(result)
    }
}

pub fn wrap_val<T: Serialize>(data: &T) -> Vec<u8> {
    serialize(&data).unwrap()
}

pub fn unwrap_val<T: Serializable>(data: &[u8]) -> T {
    deserialize(data).unwrap()
}

/// Creates a sample project data cache item.
/// Key position is generated using default partitioner.
pub fn sample_project_data_cache_item(
    uuid: &'static str,
    name: &'static str,
) -> (ProjectDataCacheKey, ProjectData) {
    let key = ProjectDataCacheKey(ProjectId::generate());
    let value = ProjectData {
        uuid: uuid.to_owned(),
        name: name.to_owned(),
        push_url: None,
        keys: vec![],
        is_enabled: true,
        is_rate_limited: true,
        allowed_origins: vec![],
        creator: Default::default(),
        verified_domains: Default::default(),
        is_verify_enabled: false,
        quota: Quota {
            max: 100000000,
            current: 0,
            is_valid: true,
        },
    };
    (key, value)
}

pub fn sample_project_data_cache_item_ser(
    uuid: &'static str,
    name: &'static str,
) -> (Vec<u8>, Vec<u8>) {
    let (key, value) = sample_project_data_cache_item(uuid, name);
    (wrap_val(&key), wrap_val(&value))
}

/// Creates multiple project data cache sample items.
/// If partitioner is not used, then key positions are consecutive numbers
/// starting from 0.
pub fn sample_project_data_cache_items(
    projects_num: usize,
) -> Vec<(ProjectDataCacheKey, ProjectData)> {
    let mut data = vec![];
    for k in 0..projects_num {
        let key = ProjectDataCacheKey(ProjectId::generate());
        let value = ProjectData {
            uuid: format!("PROJECT_UUID{k}"),
            name: format!("PROJECT_NAME{k}"),
            push_url: None,
            keys: vec![],
            is_enabled: true,
            is_rate_limited: true,
            allowed_origins: vec![],
            creator: Default::default(),
            verified_domains: Default::default(),
            is_verify_enabled: false,
            quota: Quota {
                max: 100000000,
                current: 0,
                is_valid: true,
            },
        };
        data.push((key, value));
    }
    data.sort_by(|a, b| a.0.position().cmp(&b.0.position()));
    data
}

pub fn sample_project_data_cache_items_ser(projects_num: usize) -> Vec<(Vec<u8>, Vec<u8>)> {
    sample_project_data_cache_items(projects_num)
        .into_iter()
        .map(|(k, v)| (wrap_val(&k), wrap_val(&v)))
        .collect()
}

/// Creates a sample client project routing item.
/// Key position is generated using default partitioner.
pub fn sample_client_project_routing_item() -> (ClientProjectRoutingKey, DecodedProjectId) {
    let key = ClientProjectRoutingKey(ClientId::from(
        "z6MkodHZwneVRShtaLf8JKYkxpDGp1vGZnpGmdBpX8M2exxH",
    ));
    let value = ProjectId::generate().try_into().unwrap();
    (key, value)
}

pub fn sample_client_project_routing_item_ser() -> (Vec<u8>, Vec<u8>) {
    let (key, value) = sample_client_project_routing_item();
    (wrap_val(&key), wrap_val(&value))
}

/// Creates multiple client project routing sample items.
pub fn sample_client_project_routing_items(
    clients_num: usize,
) -> Vec<(ClientProjectRoutingKey, DecodedProjectId)> {
    let mut data = vec![];
    for k in 0..clients_num {
        let key = ClientProjectRoutingKey(ClientId::from(create_client_key(k)));
        data.push((key, ProjectId::generate().try_into().unwrap()));
    }
    data.sort_by(|a, b| a.0.position().cmp(&b.0.position()));
    data
}

pub fn sample_client_project_routing_items_ser(clients_num: usize) -> Vec<(Vec<u8>, Vec<u8>)> {
    sample_client_project_routing_items(clients_num)
        .into_iter()
        .map(|(k, v)| (wrap_val(&k), wrap_val(&v)))
        .collect()
}

/// Creates multiple routing table sample items.
/// If partitioner is not used, then key positions are consecutive numbers
/// starting from 0.
pub fn sample_routing_table_items(
    topics_num: usize,
    entries_per_topic: usize,
) -> Vec<(RoutingTableKey, Vec<RoutingTableEntry>)> {
    let mut data = vec![];
    for k in 0..topics_num {
        let key = RoutingTableKey(Topic::generate());

        let mut entries = vec![];
        for i in 0..entries_per_topic {
            entries.push(RoutingTableEntry {
                client_id: ClientId::from(create_client_key((k + 1) * i + i)),
                relay_addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8080),
            });
        }
        data.push((key, entries));
    }
    data.sort_by(|a, b| a.0.position().cmp(&b.0.position()));
    data
}

pub fn sample_routing_table_items_ser(
    topics_num: usize,
    entries_per_topic: usize,
) -> Vec<(Vec<u8>, Vec<Vec<u8>>)> {
    sample_routing_table_items(topics_num, entries_per_topic)
        .into_iter()
        .map(|(k, v)| (wrap_val(&k), v.iter().map(wrap_val).collect()))
        .collect()
}

/// Creates multiple unaddressed mailbox sample items.
/// If partitioner is not used, then key positions are consecutive numbers
/// starting from 0.
pub fn sample_unaddressed_mailbox_items(
    topics_num: usize,
    msgs_per_topic: usize,
) -> Vec<(MailboxKey, Vec<MailboxMessage>)> {
    let mut data = vec![];
    for _ in 0..topics_num {
        let key = MailboxKey(Topic::generate(), None);
        let mut entries = vec![];
        for k in 0..msgs_per_topic {
            let sender_id = ClientId::from(create_client_key(msgs_per_topic + k));
            let recipient_id = ClientId::from(create_client_key(k));
            entries.push(sample_msg(
                &sender_id,
                &recipient_id,
                &MessageId::new(k as u64),
                &key.0,
                30,
            ));
        }
        data.push((key, entries));
    }
    data.sort_by(|a, b| a.0.position().cmp(&b.0.position()));
    data
}

pub fn sample_unaddressed_mailbox_items_ser(
    topics_num: usize,
    msgs_per_topic: usize,
) -> Vec<(Vec<u8>, Vec<Vec<u8>>)> {
    sample_unaddressed_mailbox_items(topics_num, msgs_per_topic)
        .into_iter()
        .map(|(k, v)| (wrap_val(&k), v.iter().map(wrap_val).collect()))
        .collect()
}

/// Creates multiple persistent mailbox sample items.
/// Key position is generated using default partitioner.
pub fn sample_persistent_mailbox_items(
    keys_num: usize,
    entries_per_key: usize,
) -> Vec<(MailboxKey, Vec<MailboxMessage>)> {
    let mut data = vec![];
    for k in 0..keys_num {
        let key = MailboxKey(
            Topic::generate(),
            Some(ClientId::from(create_client_key(k))),
        );
        let sender_id = ClientId::from(create_client_key(1000 * k));
        let mut entries = vec![];
        for i in 0..entries_per_key {
            entries.push(sample_msg(
                &sender_id,
                &key.1.clone().unwrap(),
                &MessageId::new(i as u64),
                &key.0,
                30,
            ));
        }
        data.push((key, entries));
    }
    data.sort_by(|a, b| a.0.position().cmp(&b.0.position()));
    data
}

pub fn sample_persistent_mailbox_items_ser(
    keys_num: usize,
    entries_per_key: usize,
) -> Vec<(Vec<u8>, Vec<Vec<u8>>)> {
    sample_persistent_mailbox_items(keys_num, entries_per_key)
        .into_iter()
        .map(|(k, v)| (wrap_val(&k), v.iter().map(wrap_val).collect()))
        .collect()
}

pub fn sample_msg(
    sender_id: &ClientId,
    recipient_id: &ClientId,
    message_id: &MessageId,
    topic: &Topic,
    ttl: u32,
) -> MailboxMessage {
    let test_push_notification = NotificationDetails {
        title: "Test notification".to_string(),
        body: "Find out more".to_string(),
        icon: None,
        url: None,
    };

    let serialized = serde_json::to_string(&test_push_notification).unwrap();
    let blob = base64::encode(serialized);

    let params = Publish {
        topic: topic.clone(),
        message: blob.into(),
        ttl_secs: ttl,
        tag: 42,
        // tag: WebhookFlag::from_tag(WebhookFlag::SIGN),
        prompt: false,
    };
    let content = Request::new(*message_id, Params::Publish(params));

    MailboxMessage {
        sender_id: Some(sender_id.clone()),
        recipient_id: Some(recipient_id.clone()),
        content,
        timestamp: 123,
    }
}
