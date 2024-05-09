output "id" {
  value = var.id
}

output "subnet_id" {
  value = aws_subnet.this.id
}

output "region" {
  value = var.region
}

output "ec2_instance_id" {
  value = aws_instance.this.id
}

output "ebs_volume_id" {
  value = aws_ebs_volume.this.id
}

output "ecs_service_name" {
  value = aws_ecs_service.this.name
}

output "ecs_cluster_name" {
  value = aws_ecs_cluster.this.name
}
