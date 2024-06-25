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
  default   = ""
  sensitive = true
}

variable "prometheus_target_peer_ids" {
  type    = list(string)
  default = []
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

variable "config_smart_contract_address" {
  type    = string
  default = null
}

variable "smart_contract_signer_mnemonic" {
  type      = string
  default   = null
  sensitive = true
}

variable "eth_rpc_url" {
  type      = string
  default   = null
  sensitive = true
}

variable "eth_address" {
  type    = string
  default = null
}

variable "rocksdb_num_batch_threads" {
  type    = number
  default = 8
}

variable "rocksdb_num_callback_threads" {
  type    = number
  default = 32
}

variable "rocksdb_max_subcompactions" {
  type    = number
  default = 8
}

variable "rocksdb_max_background_jobs" {
  type    = number
  default = 32
}

variable "rocksdb_ratelimiter" {
  type    = number
  default = 67108864
}

variable "rocksdb_increase_parallelism" {
  type    = number
  default = 16
}

variable "rocksdb_write_buffer_size" {
  type    = number
  default = 134217728
}

variable "rocksdb_max_write_buffer_number" {
  type    = number
  default = 8
}

variable "rocksdb_min_write_buffer_number_to_merge" {
  type    = number
  default = 2
}

variable "rocksdb_block_cache_size" {
  type    = number
  default = 4294967296
}

variable "rocksdb_block_size" {
  type    = number
  default = 4096
}

variable "rocksdb_row_cache_size" {
  type    = number
  default = 1073741824
}
