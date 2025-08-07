use {
    super::{
        evm,
        AccountAddress,
        Address,
        ClusterView,
        ConnectionError,
        DeploymentError,
        ReadError,
        ReadResult,
        Signer,
        WriteError,
        WriteResult,
    },
    crate::{
        keyspace,
        maintenance,
        migration,
        node_operator,
        settings,
        EncryptionKey,
        Event,
        Keyspace,
        Node,
        Ownership,
        Settings,
        View,
    },
    derive_more::derive::AsRef,
    futures::{Stream, TryStreamExt as _},
    std::{
        collections::HashMap,
        sync::{Arc, Mutex, MutexGuard},
    },
    tap::Pipe as _,
    tokio::sync::broadcast,
    tokio_stream::wrappers::BroadcastStream,
};

#[derive(Clone, Default)]
pub struct FakeRegistry {
    inner: Arc<Mutex<RegistryInner>>,
}

#[derive(Default)]
struct RegistryInner {
    next_contract_id: u8,
    contracts: HashMap<Address, Arc<Mutex<Inner>>>,
}

impl FakeRegistry {
    pub fn deployer(&self, signer: Signer) -> Deployer {
        Deployer {
            signer,
            registry: self.clone(),
        }
    }

    pub fn connector(&self, signer: Signer) -> Connector {
        Connector {
            signer,
            registry: self.clone(),
        }
    }

    fn inner(&self) -> MutexGuard<'_, RegistryInner> {
        self.inner.lock().unwrap()
    }
}

pub struct Deployer {
    signer: Signer,
    registry: FakeRegistry,
}

impl super::Deployer<FakeSmartContract> for Deployer {
    async fn deploy(
        &self,
        initial_settings: Settings,
        initial_operators: Vec<node_operator::Serialized>,
    ) -> Result<FakeSmartContract, DeploymentError> {
        let operator_indexes = (0..initial_operators.len()).map(|idx| idx as u8).collect();

        let keyspace = Keyspace::new(
            operator_indexes,
            keyspace::ReplicationStrategy::UniformDistribution,
            0,
        )
        .map_err(|err| DeploymentError(err.to_string()))?;

        let node_operators = initial_operators
            .clone()
            .into_iter()
            .map(|op| (op.id, op))
            .collect();

        let view = ClusterView {
            node_operators: initial_operators.into_iter().map(Some).collect(),
            ownership: Ownership::new(self.signer.address),
            settings: initial_settings,
            keyspace,
            migration: None,
            maintenance: None,
            cluster_version: 0,
        };

        let view = View::from_sc(&Config::dummy(), view)
            .await
            .map_err(|err| DeploymentError(err.to_string()))?;

        let mut registry = self.registry.inner();
        registry.next_contract_id += 1;

        let contract_address = Address(evm::Address::repeat_byte(registry.next_contract_id));

        let contract = Arc::new(Mutex::new(Inner {
            address: contract_address,
            node_operators,
            view,
            next_migration_id: 1,
            events: broadcast::channel(100).0,
        }));

        let _ = registry
            .contracts
            .insert(contract_address, contract.clone());

        Ok(FakeSmartContract {
            signer: self.signer.clone(),
            inner: contract,
        })
    }
}

pub struct Connector {
    signer: Signer,
    registry: FakeRegistry,
}

impl super::Connector<FakeSmartContract> for Connector {
    async fn connect(&self, address: Address) -> Result<FakeSmartContract, ConnectionError> {
        let contract = self
            .registry
            .inner()
            .contracts
            .get(&address)
            .cloned()
            .ok_or(ConnectionError::UnknownContract)?;

        Ok(FakeSmartContract {
            signer: self.signer.clone(),
            inner: contract,
        })
    }
}

#[derive(AsRef)]
struct Config {
    #[as_ref]
    encryption_key: EncryptionKey,
}

impl Config {
    fn dummy() -> Self {
        Self {
            encryption_key: crate::testing::encryption_key(),
        }
    }
}

impl crate::Config for Config {
    type SmartContract = FakeSmartContract;
    type KeyspaceShards = ();
    type Node = Node;

    fn new_node(&self, _operator_id: node_operator::Id, node: Node) -> Self::Node {
        node
    }
}

#[derive(Clone)]
pub struct FakeSmartContract {
    signer: Signer,
    inner: Arc<Mutex<Inner>>,
}

impl FakeSmartContract {
    fn inner(&self) -> MutexGuard<'_, Inner> {
        self.inner.lock().unwrap()
    }
}

impl FakeSmartContract {
    async fn write<Ev>(&self, f: impl FnOnce(&mut Inner) -> Ev) -> WriteResult<()>
    where
        Ev: Into<Event>,
    {
        let (mut view, event) = {
            let mut this = self.inner();
            (this.view.clone(), f(&mut this).into())
        };

        view = view
            .apply_event(&Config::dummy(), event.clone())
            .await
            .map_err(|err| WriteError::Other(err.to_string()))?;

        let mut this = self.inner();
        this.view = view;

        let _ = this.events.send(event);

        Ok(())
    }

    fn insert_node_operator(&self, operator: node_operator::Serialized) {
        let _ = self
            .inner
            .lock()
            .unwrap()
            .node_operators
            .insert(operator.id, operator);
    }

    fn remove_node_operator(&self, id: node_operator::Id) {
        let _ = self.inner.lock().unwrap().node_operators.remove(&id);
    }
}

struct Inner {
    address: Address,

    node_operators: HashMap<node_operator::Id, node_operator::Serialized>,

    view: View<Config>,

    next_migration_id: migration::Id,

    events: broadcast::Sender<Event>,
}

impl Inner {
    fn next_migration_id(&mut self) -> migration::Id {
        let id = self.next_migration_id;
        self.next_migration_id += 1;
        id
    }
}

impl super::Write for FakeSmartContract {
    fn signer(&self) -> Option<&Signer> {
        Some(&self.signer)
    }

    async fn start_migration(&self, new_keyspace: Keyspace) -> WriteResult<()> {
        self.write(|this| migration::Started {
            migration_id: this.next_migration_id(),
            new_keyspace,
            cluster_version: this.view.cluster_version + 1,
        })
        .await
    }

    async fn complete_migration(&self, id: migration::Id) -> WriteResult<()> {
        self.write(|this| {
            let event: Event = if this.view.migration().unwrap().pulling_count() > 1 {
                migration::DataPullCompleted {
                    migration_id: id,
                    operator_id: self.signer.address,
                    cluster_version: this.view.cluster_version + 1,
                }
                .into()
            } else {
                migration::Completed {
                    migration_id: id,
                    operator_id: self.signer.address,
                    cluster_version: this.view.cluster_version + 1,
                }
                .into()
            };

            event
        })
        .await
    }

    async fn abort_migration(&self, id: migration::Id) -> WriteResult<()> {
        self.write(|this| migration::Aborted {
            migration_id: id,
            cluster_version: this.view.cluster_version + 1,
        })
        .await
    }

    async fn start_maintenance(&self) -> WriteResult<()> {
        self.write(|this| maintenance::Started {
            by: self.signer.address,
            cluster_version: this.view.cluster_version + 1,
        })
        .await
    }

    async fn finish_maintenance(&self) -> WriteResult<()> {
        self.write(|this| maintenance::Finished {
            cluster_version: this.view.cluster_version + 1,
        })
        .await
    }

    async fn add_node_operator(&self, operator: node_operator::Serialized) -> WriteResult<()> {
        self.insert_node_operator(operator.clone());

        self.write(|this| node_operator::Added {
            idx: this.view.node_operators().free_idx().unwrap(),
            operator,
            cluster_version: this.view.cluster_version + 1,
        })
        .await
    }

    async fn update_node_operator(&self, operator: node_operator::Serialized) -> WriteResult<()> {
        self.insert_node_operator(operator.clone());

        self.write(|this| node_operator::Added {
            idx: this.view.node_operators().get_idx(&operator.id).unwrap(),
            operator,
            cluster_version: this.view.cluster_version + 1,
        })
        .await
    }

    async fn remove_node_operator(&self, id: node_operator::Id) -> WriteResult<()> {
        self.remove_node_operator(id);

        self.write(|this| node_operator::Removed {
            id,
            cluster_version: this.view.cluster_version + 1,
        })
        .await
    }

    async fn update_settings(&self, new_settings: Settings) -> WriteResult<()> {
        self.write(|this| settings::Updated {
            settings: new_settings,
            cluster_version: this.view.cluster_version + 1,
        })
        .await
    }
}

impl super::Read for FakeSmartContract {
    fn address(&self) -> Address {
        self.inner().address
    }

    async fn cluster_view(&self) -> ReadResult<ClusterView> {
        let this = self.inner();

        Ok(ClusterView {
            node_operators: this
                .view
                .node_operators
                .slots()
                .iter()
                .map(|slot| {
                    slot.as_ref()
                        .map(|op| this.node_operators.get(&op.id).unwrap().clone())
                })
                .collect(),
            ownership: this.view.ownership.clone(),
            settings: this.view.settings.clone(),
            keyspace: (*this.view.keyspace).clone(),
            migration: this.view.migration.clone(),
            maintenance: this.view.maintenance.clone(),
            cluster_version: this.view.cluster_version,
        })
    }

    async fn events(&self) -> ReadResult<impl Stream<Item = ReadResult<Event>> + Send + 'static> {
        BroadcastStream::new(self.inner().events.subscribe())
            .map_err(|err| ReadError::Other(err.to_string()))
            .pipe(Ok)
    }
}

pub fn signer(n: u8) -> Signer {
    let mut buf = [1; 32];
    buf[31] = n;

    Signer::try_from_private_key(&hex::encode(buf)).unwrap()
}

pub fn account_address(n: u8) -> AccountAddress {
    signer(n).address
}
