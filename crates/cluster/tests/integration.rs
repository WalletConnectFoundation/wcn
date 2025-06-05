use {
    alloy::providers::ProviderBuilder,
    cluster::{
        node,
        node_operator,
        smart_contract::{self, evm, RpcUrl, Signer},
        Cluster,
        NodeOperator,
        Settings,
    },
    libp2p::PeerId,
    std::net::SocketAddrV4,
};

#[tokio::test]
async fn test_suite() {
    let settings = Settings {
        max_node_operator_data_bytes: 4096,
    };

    // let signer =
    //     Signer::try_from_private_key(&std::env::var("SIGNER_PRIVATE_KEY").
    // unwrap()).unwrap();

    let signer = Signer::try_from_private_key(
        "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80",
    )
    .unwrap();

    // let rpc_url = "https://mainnet.optimism.io".parse().unwrap();

    let rpc_url = "http://127.0.0.1:8545".parse().unwrap();

    let operators = (1..=5).map(|n| test_node_operator(n)).collect();

    let contract = Cluster::<evm::SmartContract>::deploy(signer, rpc_url, settings, operators)
        .await
        .unwrap();

    // let contract_address = "0xcefa8836c9cd102c3b118b7681cf09d839cb324e"
    //     .parse()
    //     .unwrap();

    // let contract = Cluster::<evm::SmartContract>::connect(contract_address,
    // signer, rpc_url)     .await
    //     .unwrap();
}

fn test_node_operator(n: u8) -> NodeOperator {
    let data = node_operator::Data {
        name: node_operator::Name::new(format!("Operator{n}")).unwrap(),
        nodes: vec![node::Data {
            peer_id: PeerId::random(),
            addr: SocketAddrV4::new([127, 0, 0, 1].into(), 40000 + n as u16),
        }],
        clients: vec![],
    };

    let mut private_key: [u8; 32] = Default::default();
    private_key[0] = n;

    let signer = Signer::try_from_private_key(&format!("0x{}", hex::encode(&private_key))).unwrap();

    NodeOperator::new(signer.address(), data)
}
