resource "aws_ec2_transit_gateway_route_table_association" "peering-association" {
  transit_gateway_attachment_id  = var.transit_gateway_peering_attachment_id
  transit_gateway_route_table_id = var.transit_gateway_route_table_id
}

resource "aws_ec2_transit_gateway_route" "tgw-route" {
  destination_cidr_block         = var.peer_cidr_block
  transit_gateway_attachment_id  = var.transit_gateway_peering_attachment_id
  transit_gateway_route_table_id = var.transit_gateway_route_table_id
}

resource "aws_route" "vpc-route" {
  count = (length(var.vpc_route_table_ids))

  route_table_id         = var.vpc_route_table_ids[count.index]
  destination_cidr_block = var.peer_cidr_block
  gateway_id             = var.transit_gateway_id

  lifecycle {
    ignore_changes = [gateway_id, transit_gateway_id]
  }
}
