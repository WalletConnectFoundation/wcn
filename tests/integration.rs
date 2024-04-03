use {
    irn::cluster::keyspace::partitioner::{DefaultPartitioner, Partitioner},
    rand::Rng,
    relay_rocks::{db::schema::GenericKey, UnixTimestampSecs},
    serde::{Deserialize, Serialize},
    std::hash::{DefaultHasher, Hash, Hasher},
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

fn serialize<T: Serialize>(data: &T) -> Vec<u8> {
    rmp_serde::to_vec(data).unwrap()
}

#[derive(Debug, Clone, Hash, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
struct Data(pub Vec<u8>);

impl Data {
    pub fn generate() -> Self {
        let mut rng = rand::thread_rng();
        let data = rng.gen::<[u8; 32]>();
        Self(data.into())
    }

    pub fn generate_ser() -> Vec<u8> {
        serialize(&Self::generate())
    }

    pub fn to_hash(&self) -> Vec<u8> {
        let mut hasher = DefaultHasher::new();
        self.0.hash(&mut hasher);
        hasher.finish().to_be_bytes().into()
    }
}

fn generate_kv_data_ser() -> (Vec<u8>, Vec<u8>) {
    (Data::generate_ser(), Data::generate_ser())
}

fn generate_data(num_keys: usize, num_values: usize) -> Vec<(Data, Vec<Data>)> {
    (0..num_keys)
        .map(|_| {
            let key = Data::generate();
            let values = (0..num_values).map(|_| Data::generate()).collect();
            (key, values)
        })
        .collect()
}

fn sort_data(mut data: Vec<Vec<u8>>) -> Vec<Vec<u8>> {
    data.sort();
    data
}
