use {
    crate::utils,
    irn_api::{
        client,
        namespace::{Auth, PublicKey},
        Client,
        Key,
        SigningKey,
    },
    std::{net::SocketAddr, str::FromStr, time::Duration},
};

const MIN_TTL: Duration = Duration::from_secs(30);

#[derive(Debug, thiserror::Error)]
enum Error {
    #[error("Failed to initialize storage namespace")]
    Namespace,

    #[error("Invalid key encoding: must be base64")]
    KeyEncoding,

    #[error("Invalid key length: must be 32 byte ed25519 private key")]
    KeyLength,

    #[error("Invalid TTL: {0}")]
    Ttl(humantime::DurationError),

    #[error("Invalid TTL: Minimum TTL: {MIN_TTL:?}")]
    TtlTooShort,

    #[error("Failed to decode parameter: {0}")]
    Decoding(&'static str),

    #[error("Failed to execute storage request: {0}")]
    Client(#[from] client::Error),

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

    #[clap(short, long, env = "IRN_STORAGE_ADDRESS")]
    /// IRN node address to connect.
    ///
    /// The address is specified in format `{PEER_ID}_{MULTIADDRESS}`.
    address: SocketAddr,

    #[clap(long, env = "IRN_STORAGE_PRIVATE_KEY")]
    /// Client private key used for authorization.
    private_key: PrivateKey,

    #[clap(long, env = "IRN_STORAGE_NAMESPACE_SECRET")]
    /// Secret key to initialize namespaces.
    ///
    /// Single secret key can be used for multiple namespaces, while specific
    /// namespace is set with the `namespace` parameter.
    namespace_secret: Option<String>,

    #[clap(long, env = "IRN_STORAGE_NAMESPACE", requires("namespace_secret"))]
    /// Namespace to store and retrieve data from.
    namespace: Option<String>,

    #[clap(short, long, env = "IRN_STORAGE_ENCODING", value_enum, default_value_t = Encoding::Text)]
    /// Encoding to use when parsing input parameters (`key`, `field`, `value`)
    /// and the output data.
    ///
    /// Text encoding assumes utf8 strings, and will return an error when
    /// attempting to print any other data. To store and retrieve binary, either
    /// hex or base64 encoding should be used.
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

#[derive(Debug, Clone, Copy)]
struct Ttl(Duration);

impl FromStr for Ttl {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        humantime::parse_duration(s)
            .map_err(Error::Ttl)
            .and_then(|ttl| {
                if ttl < MIN_TTL {
                    Err(Error::TtlTooShort)
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
    HFields(HFieldsCmd),
    HVals(HValsCmd),
}

#[derive(Debug, clap::Args)]
/// Set `key` to hold the string value.
struct SetCmd {
    key: String,

    value: String,

    #[clap(short, long)]
    ttl: Option<Ttl>,
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
    ttl: Option<Ttl>,
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
    namespace: Option<PublicKey>,
    client: Client,
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

    fn key(&self, bytes: Vec<u8>) -> Key {
        Key {
            namespace: self.namespace,
            bytes,
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

    async fn del(&self, args: DelCmd) -> Result<(), Error> {
        let key = self.decode(&args.key, "key")?;

        self.client.del(self.key(key)).await?;

        Ok(())
    }

    async fn hset(&self, args: HSetCmd) -> Result<(), Error> {
        let key = self.decode(&args.key, "key")?;
        let field = self.decode(&args.field, "field")?;
        let value = self.decode(&args.value, "value")?;

        self.client
            .hset(self.key(key), field, value, ttl_to_timestamp(args.ttl))
            .await?;

        Ok(())
    }

    async fn hget(&self, args: HGetCmd) -> Result<(), Error> {
        let key = self.decode(&args.key, "key")?;
        let field = self.decode(&args.field, "field")?;

        self.client
            .hget(self.key(key), field)
            .await?
            .map(|data| self.output(data))
            .transpose()?;

        Ok(())
    }

    async fn hdel(&self, args: HDelCmd) -> Result<(), Error> {
        let key = self.decode(&args.key, "key")?;
        let field = self.decode(&args.field, "field")?;

        self.client.hdel(self.key(key), field).await?;

        Ok(())
    }

    async fn hfields(&self, args: HFieldsCmd) -> Result<(), Error> {
        let key = self.decode(&args.key, "key")?;

        let fields = self.client.hfields(self.key(key)).await?;

        for field in fields {
            self.output(field)?;
        }

        Ok(())
    }

    async fn hvals(&self, args: HValsCmd) -> Result<(), Error> {
        let key = self.decode(&args.key, "key")?;

        let fields = self.client.hvals(self.key(key)).await?;

        for field in fields {
            self.output(field)?;
        }

        Ok(())
    }
}

pub async fn exec(cmd: StorageCmd) -> anyhow::Result<()> {
    let namespaces = initialize_namespaces(&cmd)?;
    let namespace = namespaces.first().map(|ns| ns.public_key());

    // Currently, the client doesn't use or verify the peer ID of the provided node
    // address. So we can use any peer ID and not require it as an input parameter.
    let peer_id = network::Keypair::generate_ed25519().public().to_peer_id();
    let address = (peer_id, utils::network_addr(cmd.address));

    let client = Client::new(client::Config {
        key: cmd.private_key.0,
        nodes: [address].into(),
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
        StorageSub::Del(args) => storage.del(args).await?,
        StorageSub::Hset(args) => storage.hset(args).await?,
        StorageSub::HGet(args) => storage.hget(args).await?,
        StorageSub::HDel(args) => storage.hdel(args).await?,
        StorageSub::HFields(args) => storage.hfields(args).await?,
        StorageSub::HVals(args) => storage.hvals(args).await?,
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
