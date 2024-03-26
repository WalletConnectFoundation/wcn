output "service_security_group_id" {
  value = aws_security_group.vpc_app_ingress.id
}

output "target_group_arns" {
  value = aws_lb_target_group.target_group[*].arn
}

output "load_balancer_arns" {
  value = aws_alb.network_load_balancer[*].arn
}

output "service_name" {
  value = aws_ecs_service.app_service.name
}

output "lb_dns_names" {
  value = aws_alb.network_load_balancer[*].dns_name
}

output "lb_zone_ids" {
  value = aws_alb.network_load_balancer[*].zone_id
}

output "lb_listener_arns" {
  value = aws_lb_listener.listener[*].arn
}

output "lb_fqdns" {
  value = aws_route53_record.dns_load_balancer[*].fqdn
}
