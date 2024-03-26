use {
    crate::{
        db::{schema::GenericKey, types::map::Pair},
        util::serde::serialize,
    },
    rand::{distributions::Alphanumeric, Rng},
    serde::{Deserialize, Serialize},
};

#[derive(Clone, PartialEq, Eq, Debug, Hash, Serialize, Deserialize)]
pub struct TestKey {
    id: u64,
}

impl TestKey {
    pub fn new(id: u64) -> Self {
        Self { id }
    }
}

impl From<TestKey> for GenericKey {
    fn from(key: TestKey) -> Self {
        Self::new(key.id, serialize(&key).unwrap())
    }
}

impl From<TestKey> for Vec<u8> {
    fn from(key: TestKey) -> Self {
        serialize(&key).unwrap()
    }
}

#[derive(Clone, PartialEq, Eq, Debug, Hash, Serialize, Deserialize)]
pub struct TestValue {
    name: String,
}

impl TestValue {
    pub fn new(name: impl Into<String>) -> Self {
        Self { name: name.into() }
    }

    pub fn into_vec(self) -> Vec<u8> {
        serialize(&self).unwrap()
    }
}

impl From<TestValue> for Vec<u8> {
    fn from(value: TestValue) -> Self {
        serialize(&value).unwrap()
    }
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Debug, Hash)]
pub struct TestMapValue {
    field: TestValue,
    value: TestValue,
}

impl From<TestMapValue> for Vec<u8> {
    fn from(value: TestMapValue) -> Self {
        serialize(&value).unwrap()
    }
}

impl From<TestMapValue> for Pair<Vec<u8>, Vec<u8>> {
    fn from(v: TestMapValue) -> Self {
        Self::new(v.field.into(), v.value.into())
    }
}

impl TestMapValue {
    pub fn new(field: impl Into<TestValue>, value: impl Into<TestValue>) -> Self {
        Self {
            field: field.into(),
            value: value.into(),
        }
    }

    pub fn field(&self) -> TestValue {
        self.field.clone()
    }

    pub fn value(&self) -> TestValue {
        self.value.clone()
    }

    pub fn generate() -> Self {
        let field: String = rand::thread_rng()
            .sample_iter(&Alphanumeric)
            .take(10)
            .map(char::from)
            .collect();
        let value: String = rand::thread_rng()
            .sample_iter(&Alphanumeric)
            .take(30)
            .map(char::from)
            .collect();

        Self::new(TestValue::new(field), TestValue::new(value))
    }
}
