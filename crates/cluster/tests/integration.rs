use {
    alloy::providers::ProviderBuilder,
    cluster::contract::{
        self,
        Cluster::{NodeOperatorView, Settings},
    },
};

#[tokio::test]
async fn test_suite() {
    let provider = ProviderBuilder::new().on_anvil_with_wallet();

    let settings = Settings {
        minOperators: 5,
        minNodes: 1,
    };

    let contract = contract::Cluster::deploy(&provider, settings).await?;

    // // 2. Use Anvil's first default private key
    // let wallet: LocalWallet =
    // "0x59c6995e998f97a5a0044976f51e2bc4a26b19f3c6d8ff2b67d46e2b4103b3b3"
    //     .parse::<LocalWallet>()?
    //     .with_chain_id(31337);

    // let client = Arc::new(provider.with_signer(wallet));

    // // 3. Deploy Counter contract
    // let deployer = Counter::deploy(client.clone(), ())?; // No constructor
    // args let contract = deployer.send().await?;
}
