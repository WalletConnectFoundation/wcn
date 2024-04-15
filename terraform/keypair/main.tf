locals {
  name         = "${var.environment}-${var.node_id}"
  staging_name = "staging-${var.node_id}"
}

# Ed25519 secret key, 32 bytes 
resource "random_bytes" "secret_key" {
  length = 32
}

resource "aws_secretsmanager_secret" "secret_key" {
  name = local.name

  # Otherwise Terraform won't be able to delete it
  recovery_window_in_days = 0

  tags = merge(var.tags, { Name = local.name })
}

resource "aws_secretsmanager_secret_version" "secret_key" {
  secret_id     = aws_secretsmanager_secret.secret_key.id
  secret_string = random_bytes.secret_key.base64
}

data "libp2p_peer_id" "this" {
  ed25519_secret_key = aws_secretsmanager_secret_version.secret_key.secret_string
}

data "aws_secretsmanager_secret" "staging_secret_key" {
  name = local.staging_name
}

data "aws_secretsmanager_secret_version" "staging_secret_key" {
  secret_id = data.aws_secretsmanager_secret.staging_secret_key.id
}

data "libp2p_peer_id" "staging" {
  ed25519_secret_key = data.aws_secretsmanager_secret_version.staging_secret_key.secret_string
}
