#[derive(Debug, clap::Args)]
pub struct NodeConfigCmd;

pub fn exec(_args: NodeConfigCmd) -> anyhow::Result<()> {
    Ok(())
}
