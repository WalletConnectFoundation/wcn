use {
    alloy::{
        hex::FromHex,
        node_bindings::Anvil,
        primitives::{Address, Bytes},
        providers::{ext::AnvilApi, ProviderBuilder},
    },
    contract::{
        new_local_signer,
        NodePerformanceData,
        PerformanceData,
        PerformanceReporter,
        SignerType,
        StakeValidator,
    },
    metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle},
};

async fn setup_chain_state() -> (alloy::node_bindings::AnvilInstance, Vec<Address>) {
    let anvil = Anvil::new().spawn();
    let rpc_url = anvil.endpoint();

    let provider = ProviderBuilder::new().on_http(rpc_url.parse().unwrap());

    let data = std::fs::read_to_string("../../infra/anvil_state")
        .unwrap()
        .trim()
        .replace("\"", "")
        .to_string();

    let data = Bytes::from_hex(data).unwrap();

    let state_loaded = provider.anvil_load_state(data).await.unwrap();
    assert!(state_loaded);

    let addresses = anvil.addresses().to_owned();

    (anvil, addresses)
}

#[tokio::test]
async fn test_setup() {
    let prometheus = PrometheusBuilder::new().install_recorder().unwrap();

    let (anvil, addresses) = setup_chain_state().await;
    let rpc_url = anvil.endpoint();

    let contract_addr = "0xe7f1725E7734CE288F8367e1Bb143E90bb3F0512";
    let signer_mnemonic = "test test test test test test test test test test test junk";

    let signer = new_local_signer(signer_mnemonic).unwrap();

    performance_reporter_test_suite(&rpc_url, contract_addr, addresses, signer.clone()).await;

    stake_validator_test_suite(&rpc_url, contract_addr, prometheus).await;
}

async fn performance_reporter_test_suite(
    rpc_url: &str,
    contract_addr: &str,
    addresses: Vec<Address>,
    signer: impl SignerType,
) {
    let reporter = contract::new_performance_reporter(&rpc_url, contract_addr, signer)
        .await
        .unwrap();

    test_misconfiguration_reporting(&reporter).await;
    test_valid_performance_reporting(&reporter, addresses.clone()).await;
    test_invalid_data_reporting(&reporter, addresses.clone()).await;
}

async fn test_valid_performance_reporting(
    reporter: &impl PerformanceReporter,
    addresses: Vec<Address>,
) {
    let node_performances = addresses
        .into_iter()
        .map(|addr| NodePerformanceData::new(addr.to_string().as_str(), 10).unwrap())
        .collect::<Vec<NodePerformanceData>>();

    let performance_data = PerformanceData {
        epoch: 0,
        nodes: node_performances,
    };

    reporter.report_performance(performance_data).await.unwrap();
}

async fn test_misconfiguration_reporting(reporter: &impl PerformanceReporter) {}

async fn test_invalid_data_reporting(reporter: &impl PerformanceReporter, addresses: Vec<Address>) {
    let performance_data = PerformanceData {
        epoch: 0,
        nodes: vec![],
    };

    let res = reporter.report_performance(performance_data).await;
    dbg!(&res);
    assert!(res.is_err());
}

async fn test_invalid_signer_reporting(reporter: &impl PerformanceReporter) {
    //
}

async fn stake_validator_test_suite(
    rpc_url: &str,
    contract_addr: &str,
    prometheus: PrometheusHandle,
) {
    let stake_validator = StakeValidator::new(rpc_url, contract_addr).await.unwrap();
}

fn extract_metric_counts() {}
