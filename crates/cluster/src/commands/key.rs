#![allow(warnings)]

use {
    crate::commands::{CliError, SharedArgs},
    base64::Engine,
    libp2p_identity::Keypair,
};

#[derive(Debug, clap::Args)]
pub struct KeyCmd {
    #[clap(long = "seed", short = 's')]
    /// Seed used to generate keypair. If not provided, a random keypair will be
    /// generated.
    seed: Option<String>,

    #[command(subcommand)]
    commands: KeySub,
}

#[derive(clap::Subcommand, Debug)]
pub enum KeySub {
    Generate,
}

pub async fn exec(cmd: KeyCmd) -> anyhow::Result<()> {
    match cmd.commands {
        KeySub::Generate => {
            generate_new_keypair()?;
        }
    }

    Ok(())
}

fn generate_new_keypair() -> anyhow::Result<()> {
    let secret_bytes: [u8; 32] = rand::random();
    let sekey = base64::engine::general_purpose::STANDARD.encode(secret_bytes);

    let keypair = Keypair::ed25519_from_bytes(secret_bytes)?;

    let raw_bytes = keypair.public().try_into_ed25519()?.to_bytes();
    let pubkey = base64::engine::general_purpose::STANDARD.encode(raw_bytes);
    let peer_id = keypair.public().to_peer_id();

    println!("Seed: {sekey}");
    println!("Public Key: {pubkey}");
    println!("Peer Id: {peer_id}");

    Ok(())
}
