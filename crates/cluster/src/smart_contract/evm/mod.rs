use {
    super::{PublicKey, Result, RpcUrl, Signer},
    crate::{node, node_operator, NewNodeOperator, Settings},
    alloy::providers::DynProvider,
};

#[rustfmt::skip]
mod bindings;

#[derive(Clone)]
pub struct SmartContract(
    bindings::Cluster::ClusterInstance<(), DynProvider, alloy::network::Ethereum>,
);

pub(crate) type Address = alloy::primitives::Address;

impl super::SmartContract for SmartContract {
    type ReadOnly = Self;

    async fn deploy(
        signer: Signer,
        rpc_url: RpcUrl,
        initial_settings: Settings,
        initial_operators: Vec<NewNodeOperator>,
    ) -> Result<Self> {
        todo!()

        // let initial_settings = bindings::Cluster::Settings {
        //     maxOperatorDataBytes: 4096,
        // };

        // let initial_operators = initial_operators.into_iter().map(|key|
        // key.0).collect();

        // let smart_contract = bindings::Cluster::deploy(
        //     self.alloy_provider.clone(),
        //     initial_settings,
        //     initial_operators,
        // )
        // .await?;

        // Ok(Manager {
        //     smart_contract,
        //     alloy_provider: self.alloy_provider,
        // })
    }

    async fn connect(signer: Signer, rpc_url: RpcUrl) -> super::Result<Self> {
        // let wallet: EthereumWallet = match signer.inner {
        //     SignerInner::PrivateKey(key) => key.into(),
        // };

        // let provider = ProviderBuilder::new().wallet(wallet).on_http(rpc_url.0);

        // Self {
        //     smart_contract: (),
        //     alloy_provider: DynProvider::new(provider),
        // }

        todo!()
    }

    async fn connect_ro(rpc_url: RpcUrl) -> Result<Self::ReadOnly> {
        todo!()
    }

    fn signer(&self) -> PublicKey {
        todo!()
    }

    async fn start_migration(&self, plan: crate::migration::Plan) -> Result<()> {
        todo!()
    }

    async fn complete_migration(
        &self,
        id: crate::migration::Id,
        operator_idx: node_operator::Idx,
    ) -> super::Result<()> {
        todo!()
    }

    async fn abort_migration(&self, id: crate::migration::Id) -> Result<()> {
        todo!()
    }

    async fn start_maintenance(&self, operator_idx: node_operator::Idx) -> Result<()> {
        todo!()
    }

    async fn complete_maintenance(&self) -> Result<()> {
        todo!()
    }

    async fn abort_maintenance(&self) -> Result<()> {
        todo!()
    }

    async fn update_node_operator(
        &self,
        id: node_operator::Id,
        idx: node_operator::Idx,
        data: node_operator::SerializedData,
    ) -> super::Result<()> {
        todo!()
    }
}

impl super::ReadOnlySmartContract for SmartContract {
    async fn cluster_view(&self) -> super::Result<crate::View> {
        std::todo!()
        // let view = self.inner.getView().call().await?._0;

        // Ok(ClusterView {
        //     keyspace:
        // Keyspace::new(view.keyspace.replicationStrategy.try_into().unwrap()),
        // })
    }
}
