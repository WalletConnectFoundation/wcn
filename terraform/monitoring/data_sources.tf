resource "grafana_data_source" "prometheus" {
  type = "prometheus"
  name = "${var.environment}-relay-amp"
  url  = "https://aps-workspaces.eu-central-1.amazonaws.com/workspaces/${var.prometheus_workspace_id}/"

  json_data_encoded = jsonencode({
    httpMethod    = "GET"
    sigV4Auth     = true
    sigV4AuthType = "ec2_iam_role"
    sigV4Region   = "eu-central-1"
  })
}

moved {
  from = grafana_data_source.cloudwatch
  to   = grafana_data_source.cloudwatch-eu
}


resource "grafana_data_source" "cloudwatch-eu" {
  type = "cloudwatch"
  name = "${var.environment}-relay-cloudwatch"

  json_data_encoded = jsonencode({
    defaultRegion = "eu-central-1"
  })
}

resource "grafana_data_source" "cloudwatch-us" {
  type = "cloudwatch"
  name = "${var.environment}-us-east-1-relay-cloudwatch"

  json_data_encoded = jsonencode({
    defaultRegion = "us-east-1"
  })
}

resource "grafana_data_source" "cloudwatch-ap" {
  type = "cloudwatch"
  name = "${var.environment}-ap-southeast-1-relay-cloudwatch"

  json_data_encoded = jsonencode({
    defaultRegion = "ap-southeast-1"
  })
}
