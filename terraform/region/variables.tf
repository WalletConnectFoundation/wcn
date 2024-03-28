variable "region" {
  description = "The AWS region to deploy to."
  type        = string
}

variable "environment" {
  description = "The deployment environment (dev/staging/prod)."
  type        = string
}

variable "aws_otel_collector_ecr_repository_url" {
  type = string
}

variable "prometheus_endpoint" {
  type = string
}

variable "irn_node_log_level" {
  type = string
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

variable "irn_cache_buster" {
  type = string
}
