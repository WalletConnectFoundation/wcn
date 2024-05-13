#[derive(Debug, clap::Args)]
pub struct ConfigCmd;

pub fn exec(_args: ConfigCmd) -> anyhow::Result<()> {
    Ok(())
}
