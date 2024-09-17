// pub mod config;
pub mod cluster;
pub mod key;
pub mod node;
pub mod storage;

#[allow(clippy::large_enum_variant)]
#[derive(clap::Subcommand, Debug)]
pub enum SubCmd {
    /// Cluster subcommands.
    Cluster(cluster::Cmd),

    /// Node control subcommands
    Node(node::NodeCmd),

    /// Manage node and client keys
    Key(key::KeyCmd),

    /// Execute storage commands on a cluster
    Storage(storage::StorageCmd),
}
