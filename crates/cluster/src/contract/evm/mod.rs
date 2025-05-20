#[rustfmt::skip]
mod bindings;

mod operator_data;

use {
    alloy::{
        network::EthereumWallet,
        providers::{Provider, ProviderBuilder},
        signers::local::PrivateKeySigner,
        transports::http::reqwest,
    },
    std::{str::FromStr, sync::Arc},
};

pub struct ContractSettings {
    pub max_operator_data_bytes: u16,
}

pub struct ContractManager {
    provider: Arc<dyn Provider>,
}

impl ContractManager {
    pub fn new(signer: Signer, rpc_url: RpcUrl) -> Self {
        let wallet: EthereumWallet = match signer.inner {
            SignerInner::PrivateKey(key) => key.into(),
        };

        Self {
            provider: Arc::new(ProviderBuilder::new().wallet(wallet).on_http(rpc_url.0)),
        }
    }

    pub fn deploy_contract(&self, settings: ContractSettings) {}
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub struct Address(alloy::primitives::Address);

impl FromStr for Address {
    type Err = InvalidAddress;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        alloy::primitives::Address::from_str(s)
            .map(Address)
            .map_err(|err| InvalidAddress(err.to_string()))
    }
}

pub struct Signer {
    inner: SignerInner,
}

impl Signer {
    pub fn try_from_private_key(hex: &str) -> Result<Self, InvalidPrivateKey> {
        PrivateKeySigner::from_str(hex)
            .map(SignerInner::PrivateKey)
            .map(|inner| Self { inner })
            .map_err(|err| InvalidPrivateKey(err.to_string()))
    }
}

pub enum SignerInner {
    PrivateKey(PrivateKeySigner),
}

pub struct RpcUrl(reqwest::Url);

impl FromStr for RpcUrl {
    type Err = InvalidRpcUrl;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        reqwest::Url::from_str(s)
            .map(Self)
            .map_err(|err| InvalidRpcUrl(err.to_string()))
    }
}

impl From<ContractSettings> for bindings::Cluster::Settings {
    fn from(s: ContractSettings) -> Self {
        Self {
            maxOperatorDataBytes: s.max_operator_data_bytes,
        }
    }
}

#[derive(Debug, thiserror::Error)]
#[error("Invalid RPC URL: {0:?}")]
pub struct InvalidRpcUrl(String);

#[derive(Debug, thiserror::Error)]
#[error("Invalid private key: {0:?}")]
pub struct InvalidPrivateKey(String);

#[derive(Debug, thiserror::Error)]
#[error("Invalid address: {0:?}")]
pub struct InvalidAddress(String);

impl From<Address> for super::PublicKey {
    fn from(addr: Address) -> Self {
        // using `Debug` for raw non-checksummed format
        Self(format!("{addr:?}"))
    }
}

#[cfg(test)]
mod test {
    #[test]
    fn address_to_from_public_key_conversion() {
        todo!()
    }
}
