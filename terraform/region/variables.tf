variable "app_name" {
  description = "The name of the application."
  type        = string
}

variable "region" {
  description = "The AWS region to deploy to."
  type        = string
}

variable "environment" {
  description = "The deployment environment (dev/staging/prod)."
  type        = string
}

variable "ecr_repository_url" {
  description = "The URL of the ECR repository."
  type        = string
}

variable "ecr_app_version" {
  description = "The tag of the application image to deploy."
  type        = string
}

variable "hosted_zone_name" {
  type = string
}

variable "sign_client_ecr_repository_url" {
  type = string
}

variable "web3modal_canary_ecr_repository_url" {
  type = string
}

variable "aws_otel_collector_ecr_repository_url" {
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

variable "prometheus_endpoint" {
  type = string
}

variable "autoscaling_max_capacity" {
  type = number
}

variable "autoscaling_min_capacity" {
  type = number
}

variable "analytics_data_lake_bucket_name" {
  description = "The name of the S3 bucket for the data lake exports."
  type        = string
}

variable "analytics_data_lake_bucket_key_arn" {
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

variable "canary_project_id" {
  type = string
}

variable "canary_project_id_irn" {
  type = string
}

variable "ed25519_private_key" {
  type      = string
  sensitive = true
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

variable "storage_replication_lag_ms" {
  type = number
}

variable "auth_fqdns" {
  type = list(string)
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

variable "relay_lb_count" {
  description = "The number of Relay load balancers to use."
  type        = number
}

variable "relay_lb_deregistration_delay_secs" {
  description = "Deregistration delay of Relay load balancers in seconds"
  type        = number
}

variable "irn_node_cpu" {
  type = number
}

variable "irn_node_memory" {
  type = number
}

variable "irn_node_ecr_image_tag" {
  description = "The tag of the IRN image to deploy."
  type        = string
}

variable "irn_node_ec2_instance_type" {
  type = string
}

variable "irn_node_ec2_instance_profile" {
  type = string
}

variable "irn_node_ebs_volume_size" {
  type        = number
  description = "Size of the EBS volume in GiB"
}

variable "irn_node_ecs_task_execution_role_arn" {
  description = "ARN of the IAM role to use as ECS task execution role for an IRN node"
  type        = string
}

variable "irn_node_ecs_task_role_arn" {
  description = "ARN of the IAM role to use as ECS task role for an IRN node"
  type        = string
}

variable "irn_node_keypairs" {
  type      = map(object({ peer_id = string, secret_key = string, staging_peer_id = string }))
  sensitive = true
}

variable "ofac_countries" {
  description = "The list of countries under OFAC sanctions."
  type        = string
}

variable "rpc_rate_limit_whitelist" {
  description = "The list of project IDs to not rate limit by IP"
  type        = string
}

variable "irn_cache_buster" {
  type = string
}

variable "relay_cache_buster" {
  type = string
}

variable "irn_namespace_secret" {
  type      = string
  sensitive = true
}
