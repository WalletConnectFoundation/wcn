output "secret_key" {
  value       = data.libp2p_peer_id.this.ed25519_secret_key
  description = "Base64 encoded ed25519 secret key"
  sensitive   = true
}

output "peer_id" {
  value = data.libp2p_peer_id.this.base58
}

output "staging_peer_id" {
  value = data.libp2p_peer_id.staging.base58
}

