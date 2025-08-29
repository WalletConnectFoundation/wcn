#[allow(unused_imports)]
use {
    crate::commands::CliError,
    base64::Engine,
    libp2p_identity::Keypair,
    rand::{Rng, SeedableRng},
};

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct KeygenOutput {
    secret_key: String,
    public_key: String,
    peer_id: String,
}

impl KeygenOutput {
    pub fn to_json(&self) -> anyhow::Result<String> {
        let json = serde_json::to_string_pretty(self)?;
        Ok(json)
    }
}

#[derive(Debug, clap::Args)]
pub struct KeyCmd {
    #[command(subcommand)]
    commands: KeySub,
}

#[derive(clap::Subcommand, Debug)]
pub enum KeySub {
    Generate(GenerateCmd),
}

#[derive(Debug, clap::Args)]
pub struct GenerateCmd {
    #[clap(long = "secret-key", short = 's')]
    /// Secret key used to derive the keypair from. If not provided, a new
    /// keypair will be generated.
    secret_key: Option<String>,
}

pub async fn exec(cmd: KeyCmd) -> anyhow::Result<()> {
    match cmd.commands {
        KeySub::Generate(args) => {
            #[cfg(test)]
            let mut rng = rand::rngs::SmallRng::seed_from_u64(0);

            #[cfg(not(test))]
            let mut rng = rand::rng();

            let key_output = generate_new_keypair_with_rng(args.secret_key, &mut rng)?;
            println!("{}", key_output.to_json()?);
        }
    }

    Ok(())
}

fn generate_new_keypair_with_rng<R: rand::Rng>(
    secret: Option<String>,
    rng: &mut R,
) -> anyhow::Result<KeygenOutput> {
    let secret_bytes: [u8; 32] = match secret {
        Some(s) => {
            let decoded = base64::engine::general_purpose::STANDARD.decode(s)?;
            if decoded.len() != 32 {
                return Err(CliError::KeyLength.into());
            }

            let mut arr = [0u8; 32];
            arr.copy_from_slice(&decoded);
            arr
        }
        None => rng.random(),
    };

    let sekey = base64::engine::general_purpose::STANDARD.encode(secret_bytes);
    let keypair = Keypair::ed25519_from_bytes(secret_bytes)?;
    let raw_bytes = keypair.public().try_into_ed25519()?.to_bytes();
    let pubkey = base64::engine::general_purpose::STANDARD.encode(raw_bytes);
    let peer_id = keypair.public().to_peer_id().to_string();

    let key_output = KeygenOutput {
        secret_key: sekey,
        public_key: pubkey,
        peer_id,
    };

    Ok(key_output)
}
