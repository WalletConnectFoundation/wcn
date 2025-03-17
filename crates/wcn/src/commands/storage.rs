use {
    std::{net::SocketAddr, str::FromStr, time::Duration},
    wcn::Keypair,
    wcn_replication::storage,
    wcn_rpc::{quic, PeerAddr, PeerId},
};

const MIN_TTL: Duration = Duration::from_secs(30);
const MAX_TTL: Duration = Duration::from_secs(30 * 24 * 60 * 60); // 30 days;

#[derive(Debug, thiserror::Error)]
enum Error {
    #[error("Failed to initialize storage namespace")]
    Namespace,

    #[error("Invalid TTL: {0}")]
    Ttl(humantime::DurationError),

    #[error("Invalid TTL: Minimum TTL: {MIN_TTL:?}, Maximum TTL: {MAX_TTL:?}")]
    InvalidTtl,

    #[error("Failed to decode parameter: {0}")]
    Decoding(&'static str),

    #[error("Failed to execute storage request: {0}")]
    Client(#[from] wcn_replication::Error),

    #[error("Failed to write data to stdout")]
    Io(#[from] std::io::Error),

    #[error("Text encoding must be utf8")]
    TextEncoding,
}

#[derive(Debug, Clone, Copy, clap::ValueEnum)]
enum Encoding {
    Text,
    Base64,
    Hex,
}

#[derive(Debug, clap::Args)]
pub struct StorageCmd {
    #[command(subcommand)]
    commands: StorageSub,

    #[clap(short, long, env = "WCN_STORAGE_ADDRESS")]
    /// WCN node address to connect.
    ///
    /// The address is specified in format `{IP_ADDR}`.
    address: SocketAddr,

    #[clap(short, long, env = "WCN_STORAGE_PEER_ID")]
    /// WCN node peer ID.
    ///
    /// Required for node authentication.
    peer_id: PeerId,

    #[clap(long, env = "WCN_STORAGE_PRIVATE_KEY")]
    /// Client private key used for authorization.
    private_key: Keypair,

    #[clap(long, env = "WCN_STORAGE_NAMESPACE_SECRET")]
    /// Secret key to initialize namespaces.
    ///
    /// Single secret key can be used for multiple namespaces, while specific
    /// namespace is set with the `namespace` parameter.
    namespace_secret: Option<String>,

    #[clap(long, env = "WCN_STORAGE_NAMESPACE", requires("namespace_secret"))]
    /// Namespace to store and retrieve data from.
    namespace: Option<String>,

    #[clap(short, long, env = "WCN_STORAGE_ENCODING", value_enum, default_value_t = Encoding::Text)]
    /// Encoding to use when parsing input parameters (`key`, `field`, `value`)
    /// and the output data.
    ///
    /// Text encoding assumes utf8 strings, and will return an error when
    /// attempting to print any other data. To store and retrieve binary, either
    /// hex or base64 encoding should be used.
    encoding: Encoding,
}

#[derive(Debug, Clone, Copy)]
struct Ttl(Duration);

impl FromStr for Ttl {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        humantime::parse_duration(s)
            .map_err(Error::Ttl)
            .and_then(|ttl| {
                if ttl < MIN_TTL {
                    Err(Error::InvalidTtl)
                } else {
                    Ok(Self(ttl))
                }
            })
    }
}

#[derive(Debug, clap::Subcommand)]
#[clap(rename_all = "lowercase")]
enum StorageSub {
    Set(SetCmd),
    Get(GetCmd),
    Del(DelCmd),
    Hset(HSetCmd),
    HGet(HGetCmd),
    HDel(HDelCmd),
    HVals(HValsCmd),
}

#[derive(Debug, clap::Args)]
/// Set `key` to hold the string value.
struct SetCmd {
    key: String,

    value: String,

    #[clap(short, long)]
    ttl: Ttl,
}

#[derive(Debug, clap::Args)]
/// Get the value of `key`.
struct GetCmd {
    key: String,
}

#[derive(Debug, clap::Args)]
/// Removes the specified key.
struct DelCmd {
    key: String,
}

#[derive(Debug, clap::Args)]
/// Sets the specified field to hold `value` in the hash stored at `key``.
struct HSetCmd {
    key: String,

    field: String,

    value: String,

    #[clap(short, long)]
    ttl: Ttl,
}

#[derive(Debug, clap::Args)]
/// Returns the value associated with `field` in the hash stored at `key`.
struct HGetCmd {
    key: String,

    field: String,
}

#[derive(Debug, clap::Args)]
/// Removes the specified field from the hash stored at `key`.
struct HDelCmd {
    key: String,

    field: String,
}

#[derive(Debug, clap::Args)]
/// Returns all field names in the hash stored at `key`.
struct HFieldsCmd {
    key: String,
}

#[derive(Debug, clap::Args)]
/// Returns all values in the hash stored at `key`.
struct HValsCmd {
    key: String,
}

struct Storage {
    namespace: Option<wcn_auth::PublicKey>,
    driver: wcn_replication::Driver,
    encoding: Encoding,
}

impl Storage {
    fn decode(&self, data: &str, param: &'static str) -> Result<Vec<u8>, Error> {
        match &self.encoding {
            Encoding::Text => Ok(data.as_bytes().into()),
            Encoding::Base64 => data_encoding::BASE64
                .decode(data.as_bytes())
                .map_err(|_| Error::Decoding(param)),
            Encoding::Hex => data_encoding::HEXLOWER_PERMISSIVE
                .decode(data.as_bytes())
                .map_err(|_| Error::Decoding(param)),
        }
    }

    fn key(&self, bytes: Vec<u8>) -> storage::Key {
        match &self.namespace {
            Some(ns) => storage::Key::private(ns, bytes),
            None => storage::Key::shared(bytes),
        }
    }

    fn output(&self, data: Vec<u8>) -> Result<(), Error> {
        let data = match &self.encoding {
            Encoding::Text => String::from_utf8(data).map_err(|_| Error::TextEncoding)?,
            Encoding::Base64 => data_encoding::BASE64.encode(&data),
            Encoding::Hex => data_encoding::HEXLOWER_PERMISSIVE.encode(&data),
        };

        println!("{data}");

        Ok(())
    }

    async fn set(&self, args: SetCmd) -> Result<(), Error> {
        let entry = storage::Entry::new(
            self.key(self.decode(&args.key, "key")?),
            self.decode(&args.value, "value")?,
            args.ttl.0,
        );

        self.driver.set(entry).await?;

        Ok(())
    }

    async fn get(&self, args: GetCmd) -> Result<(), Error> {
        let key = self.decode(&args.key, "key")?;

        self.driver
            .get(self.key(key))
            .await?
            .map(|rec| self.output(rec.value))
            .transpose()?;

        Ok(())
    }

    async fn del(&self, args: DelCmd) -> Result<(), Error> {
        let key = self.decode(&args.key, "key")?;

        self.driver.del(self.key(key)).await?;

        Ok(())
    }

    async fn hset(&self, args: HSetCmd) -> Result<(), Error> {
        let entry = storage::MapEntry::new(
            self.key(self.decode(&args.key, "key")?),
            self.decode(&args.field, "field")?,
            self.decode(&args.value, "value")?,
            args.ttl.0,
        );

        self.driver.hset(entry).await?;

        Ok(())
    }

    async fn hget(&self, args: HGetCmd) -> Result<(), Error> {
        let key = self.decode(&args.key, "key")?;
        let field = self.decode(&args.field, "field")?;

        self.driver
            .hget(self.key(key), field)
            .await?
            .map(|rec| self.output(rec.value))
            .transpose()?;

        Ok(())
    }

    async fn hdel(&self, args: HDelCmd) -> Result<(), Error> {
        let key = self.decode(&args.key, "key")?;
        let field = self.decode(&args.field, "field")?;

        self.driver.hdel(self.key(key), field).await?;

        Ok(())
    }

    async fn hvals(&self, args: HValsCmd) -> Result<(), Error> {
        let key = self.decode(&args.key, "key")?;

        let fields = self.driver.hvals(self.key(key)).await?;

        for field in fields {
            self.output(field)?;
        }

        Ok(())
    }
}

pub async fn exec(cmd: StorageCmd) -> anyhow::Result<()> {
    let namespaces = initialize_namespaces(&cmd)?;
    let namespace = namespaces.first().map(|ns| ns.public_key());

    let id = cmd.peer_id;
    let addr = quic::socketaddr_to_multiaddr(cmd.address);

    let client = wcn_replication::Driver::new(wcn_replication::Config {
        keypair: cmd.private_key.0,
        connection_timeout: Duration::from_secs(1),
        operation_timeout: Duration::from_millis(2500),
        nodes: [PeerAddr::new(id, addr)].into(),
        namespaces,
        metrics_tag: "default",
    })
    .await?;

    let storage = Storage {
        namespace,
        driver: client,
        encoding: cmd.encoding,
    };

    match cmd.commands {
        StorageSub::Set(args) => storage.set(args).await?,
        StorageSub::Get(args) => storage.get(args).await?,
        StorageSub::Del(args) => storage.del(args).await?,
        StorageSub::Hset(args) => storage.hset(args).await?,
        StorageSub::HGet(args) => storage.hget(args).await?,
        StorageSub::HDel(args) => storage.hdel(args).await?,
        StorageSub::HVals(args) => storage.hvals(args).await?,
    }

    Ok(())
}

fn initialize_namespaces(cmd: &StorageCmd) -> Result<Vec<wcn_auth::Auth>, Error> {
    let mut result = Vec::new();

    if let (Some(ns), Some(ns_secret)) = (cmd.namespace.as_deref(), cmd.namespace_secret.as_deref())
    {
        let auth = wcn_auth::Auth::from_secret(ns_secret.as_bytes(), ns.as_bytes())
            .map_err(|_| Error::Namespace)?;

        result.push(auth);
    }

    Ok(result)
}
