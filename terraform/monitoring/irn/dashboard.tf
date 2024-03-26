data "jsonnet_file" "dashboard" {
  source = "${path.module}/dashboard.libsonnet"

  ext_code = {
    nodes         = jsonencode(var.nodes)
    notifications = jsonencode(var.notifications)
  }

  ext_str = {
    dashboard_title = "${var.environment} - IRN"
    dashboard_uid   = "${var.environment}-irn"

    prometheus_uid = var.prometheus_uid
    cloudwatch_uid = var.cloudwatch_uid

    environment = var.environment
  }
}

resource "grafana_dashboard" "this" {
  overwrite   = true
  message     = "Updated by Terraform"
  config_json = data.jsonnet_file.dashboard.rendered
}
