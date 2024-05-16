use {
    irn_api::{
        client,
        namespace::{Auth, PublicKey},
        Client,
        Key,
        SigningKey,
    },
    network::{Multiaddr, PeerId},
    std::{io::Write as _, str::FromStr, time::Duration},
};

#[derive(Debug, thiserror::Error)]
enum Error {
    #[error("Failed to initialize storage namespace")]
    Namespace,

    #[error("Invalid key encoding: must be base64")]
    KeyEncoding,

    #[error("Invalid key length: must be 32 byte ed25519 private key")]
    KeyLength,

    #[error("Invalid node address format: must be `PEERID_MULTIADDRESS`")]
    NodeAddress,

    #[error("Failed to parse peer ID")]
    PeerId,

    #[error("Failed to parse multiaddress")]
    Multiaddr,

    #[error("Invalid TTL: {0}")]
    Ttl(humantime::DurationError),

    #[error("Failed to decode parameter: {0}")]
    Decoding(&'static str),

    #[error("Failed to execute storage request: {0}")]
    Client(#[from] client::Error),

    #[error("Failed to write data to stdout")]
    Io(#[from] std::io::Error),
}

#[derive(Debug, Clone, Copy, clap::ValueEnum)]
enum Encoding {
    Raw,
    Base64,
    Hex,
}

#[derive(Debug, clap::Args)]
pub struct StorageCmd {
    #[command(subcommand)]
    commands: StorageSub,

    #[clap(short, long, env = "IRN_STORAGE_ADDRESS")]
    address: NodeAddress,

    #[clap(long, env = "IRN_STORAGE_PRIVATE_KEY")]
    private_key: PrivateKey,

    #[clap(long, env = "IRN_STORAGE_NAMESPACE_SECRET")]
    namespace_secret: Option<String>,

    #[clap(long, env = "IRN_STORAGE_NAMESPACE", requires("namespace_secret"))]
    namespace: Option<String>,

    #[clap(short, long, env = "IRN_STORAGE_ENCODING", value_enum, default_value_t = Encoding::Raw)]
    encoding: Encoding,
}

#[derive(Debug, Clone)]
struct PrivateKey(SigningKey);

impl FromStr for PrivateKey {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let bytes = data_encoding::BASE64
            .decode(s.as_bytes())
            .map_err(|_| Error::KeyEncoding)?[..]
            .try_into()
            .map_err(|_| Error::KeyLength)?;

        Ok(Self(irn_api::SigningKey::from_bytes(&bytes)))
    }
}

#[derive(Debug, Clone)]
struct NodeAddress((PeerId, Multiaddr));

impl FromStr for NodeAddress {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (id, addr) = s.split_once('_').ok_or(Error::NodeAddress)?;

        let id = PeerId::from_str(id).map_err(|_| Error::PeerId)?;
        let addr = Multiaddr::from_str(addr).map_err(|_| Error::Multiaddr)?;

        Ok(Self((id, addr)))
    }
}

#[derive(Debug, Clone, Copy)]
struct Ttl(Duration);

impl FromStr for Ttl {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        humantime::parse_duration(s).map(Self).map_err(Error::Ttl)
    }
}

#[derive(Debug, clap::Subcommand)]
enum StorageSub {
    Set(SetCmd),
    Get(GetCmd),
}

#[derive(Debug, clap::Args)]
struct SetCmd {
    key: String,

    value: String,

    #[clap(short, long)]
    ttl: Option<Ttl>,
}

#[derive(Debug, clap::Args)]
struct GetCmd {
    key: String,
}

struct Storage {
    namespace: Option<PublicKey>,
    client: Client,
    encoding: Encoding,
}

impl Storage {
    fn encode(&self, data: Vec<u8>) -> Vec<u8> {
        match &self.encoding {
            Encoding::Raw => data,
            Encoding::Base64 => data_encoding::BASE64.encode(&data).into(),
            Encoding::Hex => data_encoding::HEXLOWER_PERMISSIVE.encode(&data).into(),
        }
    }

    fn decode(&self, data: &str, param: &'static str) -> Result<Vec<u8>, Error> {
        match &self.encoding {
            Encoding::Raw => Ok(data.as_bytes().into()),
            Encoding::Base64 => data_encoding::BASE64
                .decode(data.as_bytes())
                .map_err(|_| Error::Decoding(param)),
            Encoding::Hex => data_encoding::HEXLOWER_PERMISSIVE
                .decode(data.as_bytes())
                .map_err(|_| Error::Decoding(param)),
        }
    }

    fn key(&self, bytes: Vec<u8>) -> Key {
        Key {
            namespace: self.namespace,
            bytes,
        }
    }

    fn output(&self, data: Vec<u8>) -> Result<(), Error> {
        let mut out = std::io::stdout();
        out.write_all(&self.encode(data))?;
        out.flush()?;
        Ok(())
    }

    async fn set(&self, args: SetCmd) -> Result<(), Error> {
        let key = self.decode(&args.key, "key")?;
        let value = self.decode(&args.value, "value")?;

        self.client
            .set(self.key(key), value, ttl_to_timestamp(args.ttl))
            .await?;

        Ok(())
    }

    async fn get(&self, args: GetCmd) -> Result<(), Error> {
        let key = self.decode(&args.key, "key")?;

        self.client
            .get(self.key(key))
            .await?
            .map(|data| self.output(data))
            .transpose()?;

        Ok(())
    }
}

pub async fn exec(cmd: StorageCmd) -> anyhow::Result<()> {
    let namespaces = initialize_namespaces(&cmd)?;
    let namespace = namespaces.first().map(|ns| ns.public_key());

    let client = Client::new(client::Config {
        key: cmd.private_key.0,
        nodes: [cmd.address.0].into(),
        shadowing_nodes: Default::default(),
        shadowing_factor: 0.0,
        request_timeout: Duration::from_secs(1),
        max_operation_time: Duration::from_millis(2500),
        connection_timeout: Duration::from_secs(1),
        udp_socket_count: 1,
        namespaces,
    })?;

    let storage = Storage {
        namespace,
        client,
        encoding: cmd.encoding,
    };

    match cmd.commands {
        StorageSub::Set(args) => storage.set(args).await?,
        StorageSub::Get(args) => storage.get(args).await?,
    }

    Ok(())
}

fn initialize_namespaces(cmd: &StorageCmd) -> Result<Vec<Auth>, Error> {
    let mut result = Vec::new();

    if let (Some(ns), Some(ns_secret)) = (cmd.namespace.as_deref(), cmd.namespace_secret.as_deref())
    {
        let auth =
            Auth::from_secret(ns_secret.as_bytes(), ns.as_bytes()).map_err(|_| Error::Namespace)?;

        result.push(auth);
    }

    Ok(result)
}

fn ttl_to_timestamp(ttl: Option<Ttl>) -> Option<u64> {
    ttl.map(|ttl| ttl.0)
        .and_then(|ttl| chrono::Duration::from_std(ttl).ok())
        .map(|ttl| (chrono::Utc::now() + ttl).timestamp() as u64)
}
