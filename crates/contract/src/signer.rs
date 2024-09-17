use {
    alloy::{
        primitives::ChainId,
        signers::{
            aws::{AwsSigner, AwsSignerError},
            k256::ecdsa::SigningKey,
            local::{coins_bip39, LocalSigner, LocalSignerError, MnemonicBuilder},
            Signer,
        },
    },
    aws_config::BehaviorVersion,
};

#[derive(Debug, thiserror::Error)]
pub enum SignerError {
    #[error("Invalid mnemonic")]
    InvalidMnemonic,

    #[error("Invalid index provided")]
    InvalidIndex,

    #[error("Local signer error: {0}")]
    LocalSigner(#[from] LocalSignerError),

    #[error("AWS signer error: {0}")]
    AwsSigner(#[from] AwsSignerError),

    #[error("Invalid key ID provided")]
    KeyId,

    #[error("Unknown error: {0}")]
    Other(String),
}

pub fn new_local_signer(signer_mnemonic: &str) -> Result<LocalSigner<SigningKey>, SignerError> {
    // TODO: replace signer with AWS signer
    // TODO: create trait to abstract AWS out of this logic
    //
    let index = 0;
    let signer = MnemonicBuilder::<coins_bip39::English>::default()
        .phrase(signer_mnemonic)
        .index(index)?
        .build()?;

    Ok(signer)
}

pub async fn new_kms_signer(
    aws_key_id: &str,
    chain_id: Option<ChainId>,
) -> Result<AwsSigner, SignerError> {
    if aws_key_id.is_empty() {
        return Err(SignerError::KeyId);
    }

    let config = aws_config::load_defaults(BehaviorVersion::latest()).await;
    let client = aws_sdk_kms::Client::new(&config);
    let signer = AwsSigner::new(client, aws_key_id.to_string(), chain_id).await?;

    Ok(signer)
}
