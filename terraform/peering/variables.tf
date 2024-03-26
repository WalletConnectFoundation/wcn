variable "transit_gateway_peering_attachment_id" {
  type = string
}

variable "transit_gateway_route_table_id" {
  type = string
}

variable "vpc_route_table_ids" {
  type = list(string)
}

variable "peer_cidr_block" {
  type = string
}

variable "transit_gateway_id" {
  type = string
}
