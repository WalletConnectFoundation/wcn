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

variable "aws_otel_collector_ecr_repository_url" {
  type = string
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
