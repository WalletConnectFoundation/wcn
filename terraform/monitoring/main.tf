locals {
  # Turns the arn into the format expected by
  # the Grafana provider e.g.
  # net/prod-relay-load-balancer/e9a51c46020a0f85
  load_balancers_eu = [for load_balancer_arn in var.load_balancers["eu-central-1"] : join("/", slice(split("/", load_balancer_arn), 1, 4))]
  # Transforms the target group into the grafana format
  target_groups_eu  = [for target_group_arn in var.target_groups["eu-central-1"] : split(":", target_group_arn)[5]]
  load_balancers_us = [for load_balancer_arn in var.load_balancers["us-east-1"] : join("/", slice(split("/", load_balancer_arn), 1, 4))]
  # Transforms the target group into the grafana format
  target_groups_us  = [for target_group_arn in var.target_groups["us-east-1"] : split(":", target_group_arn)[5]]
  load_balancers_ap = [for load_balancer_arn in var.load_balancers["ap-southeast-1"] : join("/", slice(split("/", load_balancer_arn), 1, 4))]
  # Transforms the target group into the grafana format
  target_groups_ap              = [for target_group_arn in var.target_groups["ap-southeast-1"] : split(":", target_group_arn)[5]]
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
  prometheus_uid = grafana_data_source.prometheus.uid
  cloudwatch_uid = grafana_data_source.cloudwatch-eu.uid
  nodes          = var.irn_nodes
  notifications  = local.notifications
}
