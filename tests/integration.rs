use {
    irn::cluster::keyspace::partitioner::{DefaultPartitioner, Partitioner},
    relay_rocks::{db::schema::GenericKey, UnixTimestampSecs},
};

mod migration;
mod pubsub;
mod replication;
mod scaling;
mod storage;

fn test_key(id: &[u8]) -> GenericKey {
    let key = id.to_vec();
    let pos = DefaultPartitioner::new().key_position(&key);
    GenericKey::new(pos, key)
}

fn timestamp(added: UnixTimestampSecs) -> UnixTimestampSecs {
    chrono::Utc::now().timestamp() as u64 + added
}
