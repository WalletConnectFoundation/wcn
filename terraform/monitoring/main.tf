locals {
  opsgenie_notification_channel = "NNOynGwVz"
  notifications = (
    var.environment == "prod" ?
    [{ "uid" : "${local.opsgenie_notification_channel}" }] :
    []
  )
}

module "irn" {
  source         = "./irn"
  environment    = var.environment
  prometheus_uid = data.grafana_data_source.prometheus.uid
  cloudwatch_uid = data.grafana_data_source.cloudwatch-eu.uid
  nodes          = var.irn_nodes
  notifications  = local.notifications
}
