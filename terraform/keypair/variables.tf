variable "node_id" {
  type = string
}

variable "environment" {
  type = string
}

variable "tags" {
  type    = any
  default = {}
}

variable "query_staging" {
  type    = bool
  default = true
}
