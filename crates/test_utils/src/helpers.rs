use {
    futures_util::{
        stream::{self, BoxStream},
        StreamExt,
    },
    relay_rpc::{auth, domain::Topic},
    relay_storage::{
        mailbox::{MailboxMessage, MailboxStorageError},
        StorageStream,
    },
    std::pin::Pin,
};

pub fn create_client_key(key: usize) -> String {
    const PREFIX_LEN: usize = auth::MULTICODEC_ED25519_HEADER.len();
    const TOTAL_LEN: usize = auth::MULTICODEC_ED25519_LENGTH + PREFIX_LEN;

    let key = &*format!("{key:0>32}"); // pad with zeroes to 32 bits

    let mut prefixed_data: [u8; TOTAL_LEN] = [0; TOTAL_LEN];
    prefixed_data[..PREFIX_LEN].copy_from_slice(&auth::MULTICODEC_ED25519_HEADER);
    prefixed_data[PREFIX_LEN..].copy_from_slice(key.as_bytes());

    format!(
        "{}{}",
        auth::MULTICODEC_ED25519_BASE,
        bs58::encode(prefixed_data).into_string()
    )
}

pub async fn collect_stream(
    stream: Pin<
        Box<
            dyn futures_util::Future<
                    Output = Result<StorageStream<'_, MailboxMessage>, MailboxStorageError>,
                > + '_,
        >,
    >,
) -> Vec<MailboxMessage> {
    stream
        .await
        .unwrap()
        .map(Result::unwrap)
        .collect::<Vec<_>>()
        .await
}

pub fn as_stream(topic: Topic) -> BoxStream<'static, Topic> {
    stream::iter(vec![topic]).boxed()
}
