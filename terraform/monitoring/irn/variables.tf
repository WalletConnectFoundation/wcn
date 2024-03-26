variable "environment" {
  type = string
}

variable "prometheus_uid" {
  type = string
}

variable "cloudwatch_uid" {
  type = string
}

variable "nodes" {
  type = list(object({
    id               = string
    region           = string
    ebs_volume_id    = string
    ec2_instance_id  = string
    ecs_cluster_name = string
    ecs_service_name = string
  }))
}

variable "notifications" {
  type = list(object({
    uid = string
  }))
}
