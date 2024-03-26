variable "region" {
  type    = string
  default = "eu-central-1"
}

variable "grafana_endpoint" {
  type = string
}

variable "ecr_app_version" {
  type    = string
  default = "latest"
}

variable "ed25519_private_key" {
  type      = string
  sensitive = true
}

variable "autoscaling_max_instances" {
  type = number
}

variable "autoscaling_min_instances" {
  type = number
}

variable "canary_project_id" {
  type = string
}

variable "canary_project_id_irn" {
  type = string
}

variable "statuspage_api_key" {
  type = string
}

variable "disconnect_timeout_ms" {
  type = number
}

variable "heartbeat_interval_ms" {
  type = number
}

variable "heartbeat_timeout_ms" {
  type = number
}

variable "storage_replication_lag_eu_ms" {
  type = number
}

variable "storage_replication_lag_ap_ms" {
  type = number
}

variable "storage_replication_lag_us_ms" {
  type = number
}

variable "relay_log_level" {
  type = string
}

variable "irn_node_log_level" {
  type = string
}

variable "relay_telemetry_level" {
  type = string
}

variable "relay_telemetry_sample_ratio" {
  type = number
}

variable "relay_node_cpu" {
  type = number
}

variable "relay_node_memory" {
  type = number
}

variable "irn_node_ec2_instance_type" {
  type = string
}

variable "irn_node_cpu" {
  type = number
}

variable "irn_node_memory" {
  type = number
}

variable "irn_node_ebs_volume_size" {
  type        = number
  description = "Size of the EBS volume in GiB"
}

variable "ofac_countries" {
  description = "The list of countries under OFAC sanctions."
  type        = string
}

variable "rpc_rate_limit_whitelist" {
  description = "The list of project IDs to not rate limit by IP"
  type        = string
  default     = ""
}

variable "relay_lb_count" {
  description = "The number of Relay load balancers to use."
  type        = number
  default     = 1
}

variable "relay_lb_deregistration_delay_secs" {
  description = "Deregistration delay of Relay load balancers in seconds"
  type        = number
  default     = 0
}

################################################################################
# Project Registry

variable "registry_api_endpoint" {
  type = string
}

variable "registry_api_auth_token" {
  type      = string
  sensitive = true
}

variable "project_data_cache_ttl" {
  type = number
}


################################################################################
# Webhooks

variable "webhook_activemq_primary_instance_class" {
  type = string
}

################################################################################
# Analytics configuration

variable "geoip_db_key" {
  description = "The S3 key of the GeoIP database."
  type        = string
  default     = "GeoLite2-City.mmdb"
}

variable "data_lake_bucket_name" {
  description = "The name of the data-lake bucket."
  type        = string
}

variable "data_lake_bucket_key_arn" {
  description = "The ARN of the KMS key used to encrypt the data-lake bucket."
  type        = string
}

################################################################################
# IRN

variable "irn_node_ecr_image_tag" {
  type = string
}

variable "irn_cache_buster" {
  type    = string
  default = ""
}

variable "relay_cache_buster" {
  type    = string
  default = ""
}
