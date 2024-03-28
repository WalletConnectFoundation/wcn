variable "region" {
  type    = string
  default = "eu-central-1"
}

variable "grafana_endpoint" {
  type = string
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

variable "irn_node_log_level" {
  type = string
}

variable "irn_node_ebs_volume_size" {
  type        = number
  description = "Size of the EBS volume in GiB"
}

variable "irn_node_ecr_image_tag" {
  type = string
}

variable "irn_cache_buster" {
  type    = string
  default = ""
}

