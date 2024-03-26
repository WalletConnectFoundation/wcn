data "jsonnet_file" "dashboard" {
  source = "${path.module}/dashboard.libsonnet"

  ext_code = {
    load_balancers_eu = jsonencode(local.load_balancers_eu)
    load_balancers_us = jsonencode(local.load_balancers_us)
    load_balancers_ap = jsonencode(local.load_balancers_ap)
  }

  ext_str = {
    dashboard_title = "${var.environment} - Relay NG"
    dashboard_uid   = "${var.environment}-relay-ng"
    prometheus_uid  = grafana_data_source.prometheus.uid

    fqdn          = var.fqdn
    environment   = var.environment
    notifications = jsonencode(local.notifications)

    regionalized_fqdn_eu = var.regionalized_fqdns[0]
    regionalized_fqdn_us = var.regionalized_fqdns[1]
    regionalized_fqdn_ap = var.regionalized_fqdns[2]

    cloudwatch_eu_uid = grafana_data_source.cloudwatch-eu.uid
    cloudwatch_us_uid = grafana_data_source.cloudwatch-us.uid
    cloudwatch_ap_uid = grafana_data_source.cloudwatch-ap.uid

    target_group_eu = local.target_groups_eu[0]
    target_group_us = local.target_groups_us[0]
    target_group_ap = local.target_groups_ap[0]

    ecs_service_name_eu = var.ecs_service_names[0]
    ecs_service_name_us = var.ecs_service_names[1]
    ecs_service_name_ap = var.ecs_service_names[2]
  }
}

resource "grafana_dashboard" "relay_monitoring" {
  # We are out of available alerts, so another dashboard won't fit.
  # Disabling for dev.
  count = var.environment == "dev" ? 0 : 1

  overwrite   = true
  message     = "Updated by Terraform"
  config_json = data.jsonnet_file.dashboard.rendered
}
