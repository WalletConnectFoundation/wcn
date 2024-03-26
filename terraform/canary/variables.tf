variable "ecr_repository_url" {
  type = string
}

variable "ecr_image_tag" {
  type    = string
  default = "v2.0"
}

variable "command" {
  type    = string
  default = "[\"npm\", \"run\", \"canary\"]"
}

variable "app_name" {
  type = string
}

variable "region" {
  type = string
}

variable "vpc_id" {
  type = string
}

variable "cpu" {
  type    = number
  default = 256
}

variable "private_subnet_ids" {
  type = list(string)
}

variable "environment" {
  type = string
}

variable "target_endpoint" {
  type = string
}

variable "canary_project_id" {
  type = string
}

variable "statuspage_api_key" {
  type = string
}

variable "tag" {
  type    = string
  default = "default"
}

variable "schedule" {
  type    = string
  default = "rate(1 minute)"
}
