variable "region" {
  type = string
}

variable "id" {
  type        = string
  description = "Node identifier in the form of `{az}-{idx}`"
}

variable "group_id" {
  type        = string
  description = "Node's group identifier"
}

variable "environment" {
  type = string
}

variable "vpc_id" {
  type = string
}

variable "route_table_id" {
  type = string
}

variable "image" {
  type = string
}

variable "peer_id" {
  type = string
}

variable "known_peers" {
  type = map(string)
}

variable "bootstrap_nodes" {
  type    = list(string)
  default = []
}

variable "libp2p_port" {
  type = number
}

variable "api_port" {
  type = number
}

variable "metrics_port" {
  type = number
}

variable "secret_key" {
  type      = string
  sensitive = true
}

variable "log_level" {
  type = string
}

variable "prometheus_endpoint" {
  type = string
}

variable "aws_otel_collector_image" {
  type    = string
  default = "docker.io/amazon/aws-otel-collector:v0.35.0"
}

variable "ec2_instance_profile" {
  type = string
}

variable "ecs_task_execution_role_arn" {
  type = string
}

variable "ecs_task_role_arn" {
  type = string
}

variable "security_group_ids" {
  type = list(string)
}

variable "ipv4_address" {
  type = string
}

variable "expose_public_ip" {
  type    = bool
  default = false
}

variable "eip_id" {
  type    = string
  default = null
}

variable "node_cpu" {
  type = number
}

variable "node_memory" {
  type = number
}

variable "ec2_instance_type" {
  type = string
}

variable "ebs_volume_size" {
  type        = number
  description = "Size of the EBS volume in GiB"
}

variable "cache_buster" {
  type    = string
  default = ""
}

variable "decommission_safety_switch" {
  type    = bool
  default = true
}

variable "tags" {
  type    = any
  default = {}
}

variable "authorized_raft_candidates" {
  type    = list(string)
  default = null
}

variable "authorized_clients" {
  type    = list(string)
  default = null
}

variable "enable_otel_collector" {
  type    = bool
  default = true
}

variable "enable_prometheus" {
  type    = bool
  default = false
}

variable "prometheus_image" {
  type    = string
  default = "docker.io/prom/prometheus:v2.52.0"
}

variable "prometheus_admin_password" {
  type      = string
  default   = null
  sensitive = true
}

variable "enable_grafana" {
  type    = bool
  default = false
}

variable "grafana_image" {
  type    = string
  default = "docker.io/grafana/grafana:10.1.10"
}

variable "grafana_port" {
  type    = number
  default = null
}

variable "grafana_admin_password" {
  type      = string
  default   = null
  sensitive = true
}

variable "smart_contract_address" {
  type      = string
  default   = null
}

variable "eth_rpc_url" {
  type      = string
  default   = null
  sensitive = true
}
