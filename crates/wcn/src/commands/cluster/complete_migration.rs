use anyhow::Context as _;

pub(super) async fn exec(admin_api_client: &wcn_admin_api::Client) -> anyhow::Result<()> {
    admin_api_client
        .complete_migration()
        .await
        .context("wcn_admin_api::Client::complete_migration")?;

    Ok(())
}
