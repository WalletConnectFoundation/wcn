#[derive(Debug, clap::Args)]
pub struct NodeConfigCmd;

#[tracing::instrument(skip(_args))]
pub fn exec(_args: NodeConfigCmd) -> anyhow::Result<()> {
    Ok(())
}
