use {anyhow::Context, rand_chacha::rand_core::SeedableRng as _};

#[derive(Debug, clap::Args)]
pub struct KeyCmd {
    #[command(subcommand)]
    commands: KeySub,
}

#[derive(Debug, clap::Subcommand)]
enum KeySub {
    Generate(GenerateCmd),
    Validate(ValidateCmd),
}

#[derive(Debug, clap::Args)]
/// Generate one or multiple keypairs.
///
/// The keys in output are encoded with base64. Additionally, a peer ID is
/// generated for each keypair.
struct GenerateCmd {
    #[clap(short, long)]
    /// Seed to use for generating keypairs. Must be a hex-encoded 32 byte
    /// string. If omitted, keypairs are generated from entropy.
    seed: Option<String>,

    #[clap(short, long, default_value = "1")]
    /// Number of keypairs to generate. By default a single keypair is
    /// generated.
    num_keys: Option<u32>,
}

#[derive(Debug, clap::Args)]
/// Validates the provided private key and prints the corresponding public key
/// (encoded with base64) and peer ID.
///
/// Alternatively, a string secret can be used instead of the key, which would
/// be expanded into an ed25519 key.
struct ValidateCmd {
    #[clap(short, long, default_value_t = false)]
    secret: bool,

    /// Private key or secret to validate. Private key must be encoded as
    /// base64, while secret can be any string data.
    key: String,
}

pub fn exec(cmd: KeyCmd) -> anyhow::Result<()> {
    match cmd.commands {
        KeySub::Generate(args) => generate(args),
        KeySub::Validate(args) => validate(args),
    }
}

fn generate(args: GenerateCmd) -> anyhow::Result<()> {
    let num_keys = args.num_keys.unwrap();

    if num_keys == 0 {
        anyhow::bail!("Invalid number of keys");
    }

    let seed = args
        .seed
        .map(|seed| data_encoding::HEXLOWER_PERMISSIVE.decode(seed.as_bytes()))
        .transpose()
        .context("Seed must be 32 byte hex-encoded string")?;

    let seed = seed
        .as_deref()
        .map(TryInto::try_into)
        .transpose()
        .context("Seed must be 32 byte hex-encoded string")?;

    let mut seed_rng = if let Some(seed) = seed {
        rand_chacha::ChaChaRng::from_seed(seed)
    } else {
        rand_chacha::ChaChaRng::from_entropy()
    };

    for i in 0..num_keys {
        let private_key = ed25519_dalek::SigningKey::generate(&mut seed_rng);
        let public_key = *private_key.verifying_key().as_bytes();
        let private_key = *private_key.as_bytes();

        let peer_id = network::Keypair::ed25519_from_bytes(private_key)
            .unwrap()
            .public()
            .to_peer_id();

        println!("Key {i}");
        println!(
            "Private key: {}",
            data_encoding::BASE64.encode(&private_key)
        );
        println!("Public key: {}", data_encoding::BASE64.encode(&public_key));
        println!("Peer ID: {}\n", peer_id);
    }

    Ok(())
}

fn validate(args: ValidateCmd) -> anyhow::Result<()> {
    let data = args.key.as_bytes();

    let private_key = if args.secret {
        irn_api::auth::client_key_from_secret(data).context("Failed to expand secret into key")?
    } else {
        irn_api::auth::client_key_from_bytes(data, irn_api::auth::Encoding::Base64)
            .context("Failed to decode key")?
    };

    let public_key = private_key.verifying_key();
    let peer_id = irn_api::auth::peer_id(&public_key);

    println!(
        "Public key: {}",
        data_encoding::BASE64.encode(public_key.as_bytes())
    );
    println!("Peer ID: {}\n", peer_id);

    Ok(())
}
