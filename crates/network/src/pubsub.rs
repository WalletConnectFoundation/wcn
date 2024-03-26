use {
    crate::{
        rpc::{self, Send},
        Client,
        Message,
    },
    futures::{stream, FutureExt, StreamExt},
    libp2p::PeerId,
};

impl<const ID: rpc::Id, Msg> rpc::Oneshot<ID, Msg>
where
    Msg: Message + Clone,
{
    /// Broadcasts a message to all known peers.
    pub async fn broadcast<C>(client: &C, msg: Msg)
    where
        C: AsRef<Client> + Send<Self, PeerId, Msg>,
    {
        let peer_ids: Vec<_> = client
            .as_ref()
            .connection_handlers
            .read()
            .await
            .keys()
            .copied()
            .collect();

        stream::iter(peer_ids)
            .for_each_concurrent(None, |peer_id| {
                Self::send(client, peer_id, msg.clone()).map(drop)
            })
            .await;
    }
}
