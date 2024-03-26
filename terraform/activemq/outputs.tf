output "broker_id" {
  value = aws_mq_broker.mq_primary.id
}

output "username" {
  value = local.master_username
}

output "password" {
  value = local.master_password
}

output "primary_connection_url" {
  value = "amqps://${local.master_username}:${local.master_password}@${aws_mq_broker.mq_primary.id}-1.mq.${var.region}.amazonaws.com:5671"
}

output "secondary_connection_url" {
  value = "amqps://${local.master_username}:${local.master_password}@${aws_mq_broker.mq_primary.id}-2.mq.${var.region}.amazonaws.com:5671"
}
