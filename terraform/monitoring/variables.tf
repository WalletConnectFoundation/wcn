variable "environment" {
  type = string
}

variable "prometheus_workspace_id" {
  type = string
}

variable "irn_nodes" {
  type = list(object({
    id               = string
    region           = string
    ebs_volume_id    = string
    ec2_instance_id  = string
    ecs_cluster_name = string
    ecs_service_name = string
  }))
}
