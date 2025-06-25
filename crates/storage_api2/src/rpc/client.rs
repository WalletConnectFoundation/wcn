use {
    super::*,
    crate::{operation, MapPage, Operation, Record, Result},
    futures::SinkExt,
    std::future::Future,
    wcn_rpc::client2::{Connection, DefaultConnectionHandler, DefaultRpcHandler},
};

impl<Kind> wcn_rpc::client2::Api for StorageApi<Kind>
where
    Self: Api,
{
    type ConnectionParameters = ();
    type ConnectionHandler = DefaultConnectionHandler;
    // type RpcHandler = DefaultRpcHandler;
}

async fn f<F: Future>(f: F) -> F::Output {
    f.await
}

impl<Kind> crate::StorageApi for Connection<StorageApi<Kind>>
where
    StorageApi<Kind>: wcn_rpc::client2::Api<
        ConnectionParameters = (),
        ConnectionHandler = DefaultConnectionHandler,
        // RpcHandler = DefaultRpcHandler,
    >,
{
    fn get<'a>(
        &'a self,
        get: operation::Get<'a>,
    ) -> impl Future<Output = Result<Option<Record>>> + Send {
        let req = GetRequest {
            namespace: get.namespace.into(),
            // key: get.key.0.into_owned().into(),
            keyspace_version: get.keyspace_version,
        };

        async move {
            let opt = Get::send(self, DefaultRpcHandler(&req))?.await??;

            Ok::<_, crate::Error>(opt.map(|resp| Record {
                value: Value(resp.value),
                expiration: resp.expiration.into(),
                version: resp.version.into(),
            }))
        }
    }

    async fn set(&self, set: operation::Set<'_>) -> Result<()> {
        todo!()
    }

    async fn del(&self, del: operation::Del<'_>) -> Result<()> {
        todo!()
    }

    async fn get_exp(&self, get_exp: operation::GetExp<'_>) -> Result<Option<EntryExpiration>> {
        todo!()
    }

    async fn set_exp(&self, set_exp: operation::SetExp<'_>) -> Result<()> {
        todo!()
    }

    async fn hget(&self, hget: operation::HGet<'_>) -> Result<Option<Record>> {
        todo!()
    }

    async fn hset(&self, hset: operation::HSet<'_>) -> Result<()> {
        todo!()
    }

    async fn hdel(&self, hdel: operation::HDel<'_>) -> Result<()> {
        todo!()
    }

    async fn hget_exp(&self, hget_exp: operation::HGetExp<'_>) -> Result<Option<EntryExpiration>> {
        todo!()
    }

    async fn hset_exp(&self, hset_exp: operation::HSetExp<'_>) -> Result<()> {
        todo!()
    }

    async fn hcard(&self, hcard: operation::HCard<'_>) -> Result<u64> {
        todo!()
    }

    async fn hscan(&self, hscan: operation::HScan<'_>) -> Result<MapPage> {
        todo!()
    }

    async fn execute(
        &self,
        operation: impl Into<Operation<'_>> + Send,
    ) -> Result<operation::Output> {
        todo!()
    }
}

// impl<'a> Storage for RemoteStorage<'a> {
//     type Error = Error;

//     async fn get(&self, get: operation::Get) -> Result<Option<Record>> {
//         Get::send(self.rpc_client(), self.server_addr, &GetRequest {
//             context: self.context(&get.namespace),
//             key: get.key.into(),
//         })
//         .await
//         .map(|opt| {
//             opt.map(|resp| Record {
//                 value: resp.value.into(),
//                 expiration: resp.expiration.into(),
//                 version: resp.version.into(),
//             })
//         })
//         .map_err(Into::into)
//     }

//     async fn set(&self, set: operation::Set) -> Result<()> {
//         Set::send(self.rpc_client(), self.server_addr, &SetRequest {
//             context: self.context(&set.namespace),
//             key: set.entry.key.into(),
//             value: set.entry.value.into(),
//             expiration: set.entry.expiration.into(),
//             version: set.entry.version.into(),
//         })
//         .await
//         .map_err(Into::into)
//     }

//     async fn set_exp(&self, set_exp: operation::SetExp) -> Result<()> {
//         SetExp::send(self.rpc_client(), self.server_addr, &SetExpRequest {
//             context: self.context(&set_exp.namespace),
//             key: set_exp.key.into(),
//             expiration: set_exp.expiration.into(),
//             version: set_exp.version.into(),
//         })
//         .await
//         .map_err(Into::into)
//     }

//     async fn del(&self, del: operation::Del) -> Result<()> {
//         Del::send(self.rpc_client(), self.server_addr, &DelRequest {
//             context: self.context(&del.namespace),
//             key: del.key.into(),
//             version: del.version.into(),
//         })
//         .await
//         .map_err(Into::into)
//     }

//     async fn get_exp(&self, get_exp: operation::GetExp) ->
// Result<Option<EntryExpiration>> {         GetExp::send(self.rpc_client(),
// self.server_addr, &GetExpRequest {             context:
// self.context(&get_exp.namespace),             key: get_exp.key.into(),
//         })
//         .await
//         .map(|opt| opt.map(|resp| EntryExpiration::from(resp.expiration)))
//         .map_err(Into::into)
//     }

//     async fn hget(&self, hget: operation::HGet) -> Result<Option<Record>> {
//         HGet::send(self.rpc_client(), self.server_addr, &HGetRequest {
//             context: self.context(&hget.namespace),
//             key: hget.key.into(),
//             field: hget.field.into(),
//         })
//         .await
//         .map(|opt| {
//             opt.map(|resp| Record {
//                 value: resp.value.into(),
//                 expiration: resp.expiration.into(),
//                 version: resp.version.into(),
//             })
//         })
//         .map_err(Into::into)
//     }

//     async fn hset(&self, hset: operation::HSet) -> Result<()> {
//         HSet::send(self.rpc_client(), self.server_addr, &HSetRequest {
//             context: self.context(&hset.namespace),
//             key: hset.entry.key.into(),
//             field: hset.entry.field.into(),
//             value: hset.entry.value.into(),
//             expiration: hset.entry.expiration.into(),
//             version: hset.entry.version.into(),
//         })
//         .await
//         .map_err(Into::into)
//     }

//     async fn hdel(&self, hdel: operation::HDel) -> Result<()> {
//         HDel::send(self.rpc_client(), self.server_addr, &HDelRequest {
//             context: self.context(&hdel.namespace),
//             key: hdel.key.into(),
//             field: hdel.field.into(),
//             version: hdel.version.into(),
//         })
//         .await
//         .map_err(Into::into)
//     }

//     async fn hget_exp(&self, hget_exp: operation::HGetExp) ->
// Result<Option<EntryExpiration>> {         HGetExp::send(self.rpc_client(),
// self.server_addr, &HGetExpRequest {             context:
// self.context(&hget_exp.namespace),             key: hget_exp.key.into(),
//             field: hget_exp.field.into(),
//         })
//         .await
//         .map(|opt| opt.map(|resp| EntryExpiration::from(resp.expiration)))
//         .map_err(Into::into)
//     }

//     async fn hset_exp(&self, hset_exp: operation::HSetExp) -> Result<()> {
//         HSetExp::send(self.rpc_client(), self.server_addr, &HSetExpRequest {
//             context: self.context(&hset_exp.namespace),
//             key: hset_exp.key.into(),
//             field: hset_exp.field.into(),
//             expiration: hset_exp.expiration.into(),
//             version: hset_exp.version.into(),
//         })
//         .await
//         .map_err(Into::into)
//     }

//     async fn hcard(&self, hcard: operation::HCard) -> Result<u64> {
//         HCard::send(self.rpc_client(), self.server_addr, &HCardRequest {
//             context: self.context(&hcard.namespace),
//             key: hcard.key.into(),
//         })
//         .await
//         .map(|resp| resp.cardinality)
//         .map_err(Into::into)
//     }

//     async fn hscan(&self, hscan: operation::HScan) -> Result<MapPage> {
//         let count = hscan.count;

//         let resp = HScan::send(self.rpc_client(), self.server_addr,
// &HScanRequest {             context: self.context(&hscan.namespace),
//             key: hscan.key.into(),
//             count: hscan.count,
//             cursor: hscan.cursor.map(Into::into),
//         })
//         .await
//         .map_err(Error::from)?;

//         Ok(MapPage {
//             has_next: resp.records.len() >= count as usize,
//             records: resp
//                 .records
//                 .into_iter()
//                 .map(|record| MapRecord {
//                     field: record.field.into(),
//                     value: record.value.into(),
//                     expiration: EntryExpiration::from(record.expiration),
//                     version: EntryVersion::from(record.version),
//                 })
//                 .collect(),
//         })
//     }

//     async fn execute(
//         &self,
//         operation: impl Into<crate::Operation> + Send,
//     ) -> Result<operation::Output> {
//         match operation.into() {
//             Operation::Get(get) => self.get(get).await.map(Into::into),
//             Operation::Set(set) => self.set(set).await.map(Into::into),
//             Operation::Del(del) => self.del(del).await.map(Into::into),
//             Operation::GetExp(get_exp) =>
// self.get_exp(get_exp).await.map(Into::into),
// Operation::SetExp(set_exp) => self.set_exp(set_exp).await.map(Into::into),
//             Operation::HGet(hget) => self.hget(hget).await.map(Into::into),
//             Operation::HSet(hset) => self.hset(hset).await.map(Into::into),
//             Operation::HDel(hdel) => self.hdel(hdel).await.map(Into::into),
//             Operation::HGetExp(hget_exp) =>
// self.hget_exp(hget_exp).await.map(Into::into),
// Operation::HSetExp(hset_exp) =>
// self.hset_exp(hset_exp).await.map(Into::into),
// Operation::HCard(hcard) => self.hcard(hcard).await.map(Into::into),
//             Operation::HScan(hscan) =>
// self.hscan(hscan).await.map(Into::into),         }
//     }
// }

impl From<wcn_rpc::client2::Error> for crate::Error {
    fn from(err: wcn_rpc::client2::Error) -> Self {
        Self::new(
            crate::ErrorKind::Transport,
            Some(format!("wcn_rpc::client::Error: {err}")),
        )
    }
}
