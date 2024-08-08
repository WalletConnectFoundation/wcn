use {
    crate::{
        rpc::{self, Send},
        Client,
        Message,
    },
    futures::{stream, FutureExt, StreamExt},
    libp2p::{Multiaddr, PeerId},
};

impl<const ID: rpc::Id, Msg> rpc::Oneshot<ID, Msg>
where
    Msg: Message + Clone,
{
    /// Broadcasts a message to all known peers.
    pub async fn broadcast<C>(client: &C, msg: Msg)
    where
        for<'a> C: AsRef<Client> + Send<Self, (&'a PeerId, &'a Multiaddr), Msg>,
    {
        let handlers = client.as_ref().connection_handlers.read().await.clone();

        stream::iter(handlers.keys())
            .for_each_concurrent(None, |(id, addr): &(PeerId, Multiaddr)| {
                Self::send(client, (id, addr), msg.clone()).map(drop)
            })
            .await;
    }
}
