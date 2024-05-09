#[derive(Debug, clap::Args)]
pub struct ConfigCmd {}

#[tracing::instrument(skip(_args))]
pub fn exec(_args: ConfigCmd) -> anyhow::Result<()> {
    Ok(())
}
