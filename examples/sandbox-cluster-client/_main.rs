use {
    anyhow::anyhow,
    api::{Entry, Key, StorageClient, Value},
    cerberus::project::ProjectData,
    clap::Parser as _,
    relay_rpc::domain::ProjectId,
};

#[derive(clap::Parser)]
struct Cli {
    /// Specifies the "id" of the node to make a request to.
    ///
    /// Should be in range (1..5)
    #[arg(short, long)]
    node_id: u8,

    #[command(subcommand)]
    command: Command,
}

#[derive(clap::Subcommand)]
enum Command {
    Get {
        /// Hex string up to 32 chars
        key: String,
    },
    Set {
        /// Hex string up to 32 chars
        key: String,
        /// Arbitrary string
        value: String,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    tracing_subscriber::fmt::init();

    let client = StorageClient::connect(format!("localhost:310{}", cli.node_id)).await?;
    let ctx = api::context::current();

    match cli.command {
        Command::Get { key } => client
            .get(ctx, Key::ProjectDataCache(project_id(key)))
            .await?
            .map_err(Into::into)
            .and_then(|v| match v {
                None => Ok(println!("None")),
                Some(Value::ProjectDataCache(ProjectData { name, .. })) => Ok(println!("{name}")),
                value => Err(anyhow!("Unexpected value: {value:?}")),
            })?,
        Command::Set { key, value } => {
            let entry = Entry::ProjectDataCache(project_id(key), dummy_project_data(value));
            client.set(ctx, entry, None).await??;
        }
    }

    Ok(())
}

fn project_id(key: String) -> ProjectId {
    let mut s = String::new();
    for _ in 0..32_usize.checked_sub(key.len()).unwrap() {
        s.push('0');
    }
    s.push_str(&key);
    s.into()
}

fn dummy_project_data(name: String) -> ProjectData {
    ProjectData {
        uuid: String::new(),
        creator: String::new(),
        name,
        push_url: None,
        keys: vec![],
        is_enabled: false,
        is_rate_limited: false,
        allowed_origins: vec![],
        verified_domains: vec![],
    }
}
