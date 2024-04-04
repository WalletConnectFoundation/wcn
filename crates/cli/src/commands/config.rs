#[derive(Debug, clap::Args)]
pub struct ConfigCmd {}

#[tracing::instrument(skip(args))]
pub fn exec(args: ConfigCmd) -> anyhow::Result<()> {
    Ok(())
}
