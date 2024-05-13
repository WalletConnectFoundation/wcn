pub mod config;
pub mod key;
pub mod node;

#[derive(clap::Subcommand, Debug)]
pub enum SubCmd {
    /// Node control subcommands
    Node(node::NodeCmd),

    /// Manage the CLI's configuration
    Config(config::ConfigCmd),

    /// Manage node and client keys
    Key(key::KeyCmd),
}
