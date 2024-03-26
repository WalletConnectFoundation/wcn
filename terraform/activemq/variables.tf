variable "broker_name" {
  type = string
}

variable "app_name" {
  type = string
}

variable "environment" {
  type = string
}

variable "region" {
  type = string
}

variable "primary_instance_class" {
  type = string
}

variable "allowed_ingress_cidr_blocks" {
  type = list(string)
}

variable "private_subnet_ids" {
  type = list(string)
}

variable "vpc_id" {
  type = string
}

variable "apply_immediately" {
  type    = bool
  default = false
}

variable "enable_logs" {
  type    = bool
  default = false
}
