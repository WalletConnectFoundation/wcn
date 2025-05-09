use anyhow::Context as _;

pub(super) async fn exec(admin_api_client: &wcn_admin_api::Client) -> anyhow::Result<()> {
    let view = admin_api_client
        .get_cluster_view()
        .await
        .context("wcn_admin_api::Client::get_cluster_view")?;

    let view = serde_json::to_string_pretty(&view).context("serde_json::to_string_pretty")?;

    println!("{view}");

    Ok(())
}
