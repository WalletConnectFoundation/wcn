data "grafana_data_source" "prometheus" {
  name = "${var.environment}-relay-amp"
}


data "grafana_data_source" "cloudwatch-eu" {
  name = "${var.environment}-relay-cloudwatch"
}

