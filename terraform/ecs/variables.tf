variable "ecr_repository_url" {
  description = "The URL of the ECR repository for the app image."
  type        = string
}

variable "aws_otel_collector_ecr_repository_url" {
  description = "The URL of the ECR repository for the OpenTelemetry Collector ."
  type        = string
}

variable "ecr_app_version" {
  description = "The tag of the app image to deploy."
  type        = string
}

variable "acm_certificate_arn" {
  description = "The ARN of the ACM certificate to use with the load balancer."
  type        = string
}

variable "app_name" {
  description = "The name of the app."
  type        = string
}

variable "region" {
  description = "The AWS region to deploy into."
  type        = string
}

variable "vpc_id" {
  description = "The ID of the VPC to deploy into."
  type        = string
}

variable "vpc_cidr_blocks" {
  type = list(string)
}

variable "private_subnet_ids" {
  type = list(string)
}

variable "public_subnet_ids" {
  type = list(string)
}

variable "environment" {
  type = string
}

variable "webhook_amqp_addr_primary" {
  description = "The URL of the primary webhook AMQP broker."
  type        = string
}

variable "webhook_amqp_addr_secondary" {
  description = "The URL of the secondary webhook AMQP broker."
  type        = string
}

variable "public_port" {
  type = number
}

variable "private_port" {
  type = number
}

variable "prometheus_endpoint" {
  type = string
}

variable "fqdn" {
  type = string
}

variable "route53_zone_id" {
  type = string
}

variable "autoscaling_max_capacity" {
  type = number
}

variable "autoscaling_min_capacity" {
  type = number
}

variable "analytics-data-lake_bucket_name" {
  description = "The name of the S3 bucket for the data lake exports."
  type        = string
}

variable "analytics_data_lake-bucket-key_arn" {
  description = "The ARN of the KMS key for the data lake bucket."
  type        = string
}

variable "analytics_geoip_db_bucket_name" {
  description = "The name of the S3 bucket for the GeoIP database."
  type        = string
}

variable "analytics_geoip_db_key" {
  description = "The key of the GeoIP database in the bucket."
  type        = string
}

variable "project_data_cache_ttl" {
  type = number
}

variable "registry_api_endpoint" {
  type = string
}

variable "registry_api_auth_token" {
  type      = string
  sensitive = true
}

variable "ed25519_private_key" {
  type      = string
  sensitive = true
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

variable "storage_replication_lag_ms" {
  type = number
}

variable "auth_fqdns" {
  type = list(string)
}

variable "log_level" {
  type = string
}

variable "telemetry_level" {
  type = string
}

variable "telemetry_sample_ratio" {
  type = number
}

variable "relay_node_cpu" {
  type = number
}

variable "relay_node_memory" {
  type = number
}

variable "irn_nodes" {
  type = map(string)
}

variable "irn_shadowing_nodes" {
  type = map(string)
}

variable "irn_api_port" {
  type = number
}

variable "ofac_countries" {
  description = "The list of countries under OFAC sanctions."
  type        = string
}

variable "rpc_rate_limit_whitelist" {
  description = "The list of project IDs to not rate limit by IP"
  type        = string
}

variable "lb_count" {
  description = "The number of load balancers to use."
  type        = number
}

variable "lb_deregistration_delay_secs" {
  description = "Deregistration delay of load balancers in seconds"
  type        = number
}

variable "cache_buster" {
  type = string
}

variable "irn_namespace_secret" {
  type      = string
  sensitive = true
}
