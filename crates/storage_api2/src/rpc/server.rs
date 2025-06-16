use {
    super::*,
    crate::{operation, StorageApi},
    futures::SinkExt as _,
    std::{collections::HashSet, future::Future, sync::Arc, time::Duration},
    wcn_rpc::{
        middleware::Timeouts,
        server::{
            middleware::{MeteredExt, WithTimeoutsExt},
            ClientConnectionInfo,
            ConnectionInfo,
        },
        transport::{self, BiDirectionalStream, NoHandshake, PendingConnection, PostcardCodec},
    },
};

/// Storage namespace.
pub type Namespace = Vec<u8>;

/// Storage API [`Server`] config.
pub struct Config {
    /// Name of the [`Server`].
    pub name: rpc::ServerName,

    /// Timeout of a [`Server`] operation.
    pub operation_timeout: Duration,
}

/// Storage API server.
pub trait Server: StorageApi<Error = Error> + Clone + Send + Sync + 'static {
    /// Checks whether the provided keyspace version matches the one of the [`Server`].
    fn validate_keyspace_version(&self, version: u64) -> bool;

    /// Converts this Storage API [`Server`] into an [`rpc::Server`].
    fn into_rpc_server(self, cfg: Config) -> impl rpc::Server {
        let timeouts = Timeouts::new().with_default(cfg.operation_timeout);

        let rpc_server_config = wcn_rpc::server::Config {
            name: cfg.name,
            handshake: NoHandshake,
        };

        RpcServer {
            api_server: self,
            config: rpc_server_config,
        }
        .with_timeouts(timeouts)
        .metered()
    }
}

struct RpcHandler<'a, S> {
    api_server: &'a S,
    conn_info: &'a ConnectionInfo<(), ()>,
}

impl<S: Server> RpcHandler<'_, S> {

    async fn get(&self, req: GetRequest) -> wcn_rpc::Result<Option<GetResponse>> {
        let record = self
            .api_server
            .get(operation::Get {
                namespace: 
                key: self.prepare_key(req.key)?,
            })
            .await
            .map_err(Error::into_rpc_error)?;

        Ok(record.map(|rec| GetResponse {
            value: rec.value,
            expiration: rec.expiration.timestamp(),
            version: rec.version.timestamp(),
        }))
    }

    async fn set(&self, req: SetRequest) -> wcn_rpc::Result<()> {
        let entry = Entry {
            key: self.prepare_key(req.key)?,
            value: req.value,
            expiration: EntryExpiration::from(req.expiration),
            version: EntryVersion::from(req.version),
        };

        self.api_server
            .execute_set(operation::Set { entry })
            .await
            .map_err(Error::into_rpc_error)
    }

    async fn del(&self, req: DelRequest) -> wcn_rpc::Result<()> {
        self.api_server
            .del(operation::Del {
                key: self.prepare_key(req.key)?,
                version: EntryVersion::from(req.version),
            })
            .await
            .map_err(Error::into_rpc_error)
    }

    async fn get_exp(&self, req: GetExpRequest) -> wcn_rpc::Result<Option<GetExpResponse>> {
        let expiration = self
            .api_server
            .execute_get_exp(operation::GetExp {
                key: self.prepare_key(req.key)?,
            })
            .await
            .map_err(Error::into_rpc_error)?;

        Ok(expiration.map(|exp| GetExpResponse {
            expiration: exp.timestamp(),
        }))
    }

    async fn set_exp(&self, req: SetExpRequest) -> wcn_rpc::Result<()> {
        self.api_server
            .execute_set_exp(operation::SetExp {
                key: self.prepare_key(req.key)?,
                expiration: EntryExpiration::from(req.expiration),
                version: EntryVersion::from(req.version),
            })
            .await
            .map_err(Error::into_rpc_error)
    }

    async fn hget(&self, req: HGetRequest) -> wcn_rpc::Result<Option<HGetResponse>> {
        let record = self
            .api_server
            .execute_hget(operation::HGet {
                key: self.prepare_key(req.key)?,
                field: req.field,
            })
            .await
            .map_err(Error::into_rpc_error)?;

        Ok(record.map(|rec| HGetResponse {
            value: rec.value,
            expiration: rec.expiration.timestamp(),
            version: rec.version.timestamp(),
        }))
    }

    async fn hset(&self, req: HSetRequest) -> wcn_rpc::Result<()> {
        let entry = MapEntry {
            key: self.prepare_key(req.key)?,
            field: req.field,
            value: req.value,
            expiration: EntryExpiration::from(req.expiration),
            version: EntryVersion::from(req.version),
        };

        self.api_server
            .execute_hset(operation::HSet { entry })
            .await
            .map_err(Error::into_rpc_error)
    }

    async fn hdel(&self, req: HDelRequest) -> wcn_rpc::Result<()> {
        self.api_server
            .execute_hdel(operation::HDel {
                key: self.prepare_key(req.key)?,
                field: req.field,
                version: EntryVersion::from(req.version),
            })
            .await
            .map_err(Error::into_rpc_error)
    }

    async fn hget_exp(&self, req: HGetExpRequest) -> wcn_rpc::Result<Option<HGetExpResponse>> {
        let expiration = self
            .api_server
            .execute_hget_exp(operation::HGetExp {
                key: self.prepare_key(req.key)?,
                field: req.field,
            })
            .await
            .map_err(Error::into_rpc_error)?;

        Ok(expiration.map(|exp| HGetExpResponse {
            expiration: exp.timestamp(),
        }))
    }

    async fn hset_exp(&self, req: HSetExpRequest) -> wcn_rpc::Result<()> {
        self.api_server
            .execute_hset_exp(operation::HSetExp {
                key: self.prepare_key(req.key)?,
                field: req.field,
                expiration: EntryExpiration::from(req.expiration),
                version: EntryVersion::from(req.version),
            })
            .await
            .map_err(Error::into_rpc_error)
    }

    async fn hcard(&self, req: HCardRequest) -> wcn_rpc::Result<HCardResponse> {
        self.api_server
            .execute_hcard(operation::HCard {
                key: self.prepare_key(req.key)?,
            })
            .await
            .map(|cardinality| HCardResponse { cardinality })
            .map_err(Error::into_rpc_error)
    }

    async fn hscan(&self, req: HScanRequest) -> wcn_rpc::Result<HScanResponse> {
        let page = self
            .api_server
            .execute_hscan(operation::HScan {
                key: self.prepare_key(req.key)?,
                count: req.count,
                cursor: req.cursor,
            })
            .await
            .map_err(Error::into_rpc_error)?;

        Ok(HScanResponse {
            records: page
                .records
                .into_iter()
                .map(|rec| HScanResponseRecord {
                    field: rec.field,
                    value: rec.value,
                    expiration: rec.expiration.timestamp(),
                    version: rec.version.timestamp(),
                })
                .collect(),
            has_more: page.has_next,
        })
    }
}

#[derive(Clone, Debug)]
struct RpcServer<S> {
    api_server: S,
    config: rpc::server::Config,
}

impl<S> rpc::Server for RpcServer<S>
where
    S: Server,
{
    type Handshake = NoHandshake;
    type ConnectionData = ();
    type Codec = PostcardCodec;

    fn config(&self) -> &wcn_rpc::server::Config<Self::Handshake> {
        &self.config
    }

    fn handle_rpc<'a>(
        &'a self,
        id: rpc::Id,
        stream: BiDirectionalStream,
        conn_info: &'a ClientConnectionInfo<Self>,
    ) -> impl Future<Output = ()> + Send + 'a {
        async move {
            let handler = RpcHandler {
                api_server: &self.api_server,
                conn_info,
            };

            let _ = match id {
                Get::ID => Get::handle(stream, |req| handler.get(req)).await,
                Set::ID => Set::handle(stream, |req| handler.set(req)).await,
                Del::ID => Del::handle(stream, |req| handler.del(req)).await,
                GetExp::ID => GetExp::handle(stream, |req| handler.get_exp(req)).await,
                SetExp::ID => SetExp::handle(stream, |req| handler.set_exp(req)).await,

                HGet::ID => HGet::handle(stream, |req| handler.hget(req)).await,
                HSet::ID => HSet::handle(stream, |req| handler.hset(req)).await,
                HDel::ID => HDel::handle(stream, |req| handler.hdel(req)).await,
                HGetExp::ID => HGetExp::handle(stream, |req| handler.hget_exp(req)).await,
                HSetExp::ID => HSetExp::handle(stream, |req| handler.hset_exp(req)).await,
                HCard::ID => HCard::handle(stream, |req| handler.hcard(req)).await,
                HScan::ID => HScan::handle(stream, |req| handler.hscan(req)).await,

                id => return tracing::warn!("Unexpected RPC: {}", rpc::Name::new(id)),
            }
            .map_err(
                |err| tracing::debug!(name = %rpc::Name::new(id), ?err, "Failed to handle RPC"),
            );
        }
    }
}

/// Error of a [`Server`] operation.
#[derive(Clone, Debug)]
pub struct Error(String);

impl Error {
    pub fn new<E: std::error::Error>(err: E) -> Self {
        Self(format!("{err}"))
    }

    fn into_rpc_error(self) -> wcn_rpc::Error {
        wcn_rpc::Error {
            code: "internal".into(),
            description: Some(self.0.into()),
        }
    }
}

/// [`Server`] operation [`Result`].
pub type Result<T, E = Error> = std::result::Result<T, E>;

impl From<operation::WrongOutput> for Error {
    fn from(err: operation::WrongOutput) -> Self {
        Self(err.to_string())
    }
}
