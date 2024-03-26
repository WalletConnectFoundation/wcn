output "vpc_id" {
  value = module.vpc.vpc_id
}

output "vpc_name" {
  value = module.vpc.name
}

output "private_subnets" {
  value = local.relay_private_subnet_ids
}

output "public_subnets" {
  value = module.vpc.public_subnets
}

output "transit_gateway_id" {
  value = module.tgw.ec2_transit_gateway_id
}

output "transit_gateway_route_table_id" {
  value = module.tgw.ec2_transit_gateway_route_table_id
}

output "cidr_block" {
  value = module.vpc.vpc_cidr_block
}

output "vpc_route_table_ids" {
  value = module.vpc.private_route_table_ids
}

output "lb_zone_ids" {
  value = module.ecs.lb_zone_ids
}

output "lb_dns_names" {
  value = module.ecs.lb_dns_names
}

output "lb_target_group_arns" {
  value = module.ecs.target_group_arns
}

output "lb_arns" {
  value = module.ecs.load_balancer_arns
}

output "lb_listener_arns" {
  value = module.ecs.lb_listener_arns
}

output "ecs_service_name" {
  value = module.ecs.service_name
}

output "irn_nodes" {
  value = module.irn_node
}

output "fqdn" {
  value = local.fqdn
}

output "lb_fqdns" {
  value = module.ecs.lb_fqdns
}

output "health_check_id" {
  value = aws_route53_health_check.health_check.id
}
