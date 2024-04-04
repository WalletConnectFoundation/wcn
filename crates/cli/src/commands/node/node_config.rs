#[derive(Debug, clap::Args)]
pub struct NodeConfigCmd {}

#[tracing::instrument(skip(args))]
pub fn exec(args: NodeConfigCmd) -> anyhow::Result<()> {
    Ok(())
}
