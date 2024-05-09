terraform {
  required_version = "~> 1.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = ">= 4.4"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.6"
    }
    libp2p = {
      source  = "WalletConnect/libp2p"
      version = "~> 0.1.0"
    }
  }
}
