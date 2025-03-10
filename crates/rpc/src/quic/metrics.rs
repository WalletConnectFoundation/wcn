use {
    derive_more::Deref,
    quinn_proto::{ConnectionStats, FrameStats, PathStats, UdpStats},
    std::{sync::Arc, time::Duration},
    tokio::{sync::Mutex, task::yield_now},
    wc::metrics::{self, enum_ordinalize::Ordinalize, EnumLabel, StringLabel},
};

/// Metered QUIC connection.
#[derive(Clone, Debug, Deref)]
pub struct MeteredConnection {
    #[deref]
    conn: quinn::Connection,
    conn_type: ConnectionType,

    stats: Arc<Mutex<ConnectionStats>>,

    task_handle: Arc<tokio::task::JoinHandle<()>>,
}

impl MeteredConnection {
    fn new(conn: quinn::Connection, conn_type: ConnectionType) -> Self {
        let stats = Arc::new(Mutex::new(ConnectionStats::default()));

        let task_handle = tokio::spawn({
            let stats = stats.clone();
            let conn = conn.clone();
            async move {
                let mut interval = tokio::time::interval(Duration::from_secs(15));
                loop {
                    interval.tick().await;
                    let mut stats = stats.lock().await;
                    let new_stats = conn.stats();
                    produce_connection_metrics(conn_type, Local(new_stats) - *stats).await;
                    *stats = new_stats;
                }
            }
        });

        Self {
            conn,
            conn_type,
            stats,
            task_handle: Arc::new(task_handle),
        }
    }

    /// Creates new inbound [`MeteredConnection`].
    pub fn inbound(conn: quinn::Connection) -> Self {
        Self::new(conn, ConnectionType::Inbound)
    }

    /// Creates new outbound [`MeteredConnection`].
    pub fn outbound(conn: quinn::Connection) -> Self {
        Self::new(conn, ConnectionType::Outbound)
    }
}

impl Drop for MeteredConnection {
    fn drop(&mut self) {
        self.task_handle.abort();

        let conn = self.conn.clone();
        let conn_type = self.conn_type;
        let stats = self.stats.clone();

        tokio::spawn(async move {
            produce_connection_metrics(conn_type, Local(conn.stats()) - *stats.lock().await).await;
        });
    }
}

#[derive(Clone, Copy, Debug, Ordinalize)]
enum Direction {
    Tx,
    Rx,
}

impl metrics::Enum for Direction {
    fn as_str(&self) -> &'static str {
        match self {
            Self::Tx => "tx",
            Self::Rx => "rx",
        }
    }
}

#[derive(Clone, Copy, Debug, Ordinalize)]
enum ConnectionType {
    Inbound,
    Outbound,
}

impl metrics::Enum for ConnectionType {
    fn as_str(&self) -> &'static str {
        match self {
            Self::Inbound => "inbound",
            Self::Outbound => "outbound",
        }
    }
}

async fn produce_connection_metrics(conn_type: ConnectionType, stats: ConnectionStats) {
    // TODO: Consider updating or removing.
    let _ = |dir: Direction, udp: UdpStats, frame: FrameStats| async move {
        for (stat, value) in [
            ("datagrams", udp.datagrams),
            ("bytes", udp.bytes),
            ("ios", udp.ios),
            ("acks", frame.acks),
            ("crypto", frame.crypto),
            ("connection_close", frame.connection_close),
            ("data_blocked", frame.data_blocked),
            ("datagram", frame.datagram),
            ("handshake_done", frame.handshake_done as u64),
            ("max_data", frame.max_data),
            ("max_stream_data", frame.max_stream_data),
            ("max_streams_bidi", frame.max_streams_bidi),
            ("max_streams_uni", frame.max_streams_uni),
            ("new_connection_id", frame.new_connection_id),
            ("new_token", frame.new_token),
            ("path_challenge", frame.path_challenge),
            ("path_response", frame.path_response),
            ("ping", frame.ping),
            ("reset_stream", frame.reset_stream),
            ("retire_connection_id", frame.retire_connection_id),
            ("stream_data_blocked", frame.stream_data_blocked),
            ("streams_blocked_bidi", frame.streams_blocked_bidi),
            ("streams_blocked_uni", frame.streams_blocked_uni),
            ("stop_sending", frame.stop_sending),
            ("stream", frame.stream),
        ] {
            metrics::counter!("quic_connection_stats",
                EnumLabel<"connection_type", ConnectionType> => conn_type,
                EnumLabel<"direction", Direction> => dir,
                StringLabel<"stat"> => stat
            )
            .increment(value);

            yield_now().await;
        }
    };

    // produce_bi_metrics(Direction::Tx, stats.udp_tx, stats.frame_tx).await;
    // produce_bi_metrics(Direction::Rx, stats.udp_rx, stats.frame_rx).await;

    for (stat, value) in [
        ("cwnd", stats.path.cwnd),
        ("congestion_events", stats.path.congestion_events),
        ("lost_packets", stats.path.lost_packets),
        ("lost_bytes", stats.path.lost_bytes),
        ("sent_packets", stats.path.sent_packets),
        ("sent_plpmtud_probes", stats.path.sent_plpmtud_probes),
        ("lost_plpmtud_probes", stats.path.lost_plpmtud_probes),
        ("black_holes_detected", stats.path.black_holes_detected),
    ] {
        metrics::counter!("quic_connection_path_stats",
            EnumLabel<"connection_type", ConnectionType> => conn_type,
            StringLabel<"stat"> => stat
        )
        .increment(value);

        yield_now().await;
    }

    metrics::histogram!("quic_connection_rtt",
        EnumLabel<"connection_type", ConnectionType> => conn_type
    )
    .record(stats.path.rtt);

    metrics::histogram!("quic_connection_cwnd",
        EnumLabel<"connection_type", ConnectionType> => conn_type
    )
    .record(stats.path.cwnd as f64);
}

#[derive(Deref)]
struct Local<T>(pub T);

impl std::ops::Sub<ConnectionStats> for Local<ConnectionStats> {
    type Output = ConnectionStats;

    fn sub(self, rhs: ConnectionStats) -> Self::Output {
        let mut stats = self.0;

        stats.udp_tx = Local(stats.udp_tx) - rhs.udp_tx;
        stats.udp_rx = Local(stats.udp_rx) - rhs.udp_rx;
        stats.frame_tx = Local(stats.frame_tx) - rhs.frame_tx;
        stats.frame_rx = Local(stats.frame_rx) - rhs.frame_rx;
        stats.path = Local(stats.path) - rhs.path;

        stats
    }
}

impl std::ops::Sub<UdpStats> for Local<UdpStats> {
    type Output = UdpStats;

    fn sub(self, rhs: UdpStats) -> Self::Output {
        let mut stats = self.0;

        stats.datagrams -= rhs.datagrams;
        stats.bytes -= rhs.bytes;
        stats.ios -= rhs.ios;

        stats
    }
}

impl std::ops::Sub<FrameStats> for Local<FrameStats> {
    type Output = FrameStats;

    fn sub(self, rhs: FrameStats) -> Self::Output {
        let mut stats = self.0;

        stats.acks -= rhs.acks;
        stats.crypto -= rhs.crypto;
        stats.connection_close -= rhs.connection_close;
        stats.data_blocked -= rhs.data_blocked;
        stats.datagram -= rhs.datagram;
        stats.handshake_done -= rhs.handshake_done;
        stats.max_data -= rhs.max_data;
        stats.max_stream_data -= rhs.max_stream_data;
        stats.max_streams_bidi -= rhs.max_streams_bidi;
        stats.max_streams_uni -= rhs.max_streams_uni;
        stats.new_connection_id -= rhs.new_connection_id;
        stats.new_token -= rhs.new_token;
        stats.path_challenge -= rhs.path_challenge;
        stats.path_response -= rhs.path_response;
        stats.ping -= rhs.ping;
        stats.reset_stream -= rhs.reset_stream;
        stats.retire_connection_id -= rhs.retire_connection_id;
        stats.stream_data_blocked -= rhs.stream_data_blocked;
        stats.streams_blocked_bidi -= rhs.streams_blocked_bidi;
        stats.streams_blocked_uni -= rhs.streams_blocked_uni;
        stats.stop_sending -= rhs.stop_sending;
        stats.stream -= rhs.stream;

        stats
    }
}

impl std::ops::Sub<PathStats> for Local<PathStats> {
    type Output = PathStats;

    fn sub(self, rhs: PathStats) -> Self::Output {
        let mut stats = self.0;

        stats.congestion_events -= rhs.congestion_events;
        stats.lost_packets -= rhs.lost_packets;
        stats.lost_bytes -= rhs.lost_bytes;
        stats.sent_packets -= rhs.sent_packets;
        stats.sent_plpmtud_probes -= rhs.sent_plpmtud_probes;
        stats.lost_plpmtud_probes -= rhs.lost_plpmtud_probes;
        stats.black_holes_detected -= rhs.black_holes_detected;

        stats
    }
}
