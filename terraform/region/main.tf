locals {
  cidr = terraform.workspace == "prod" ? local.prod_cidr : local.staging_cidr

  prod_cidr = {
    "eu-central-1"   = "/16"
    "us-east-1"      = "/16"
    "ap-southeast-1" = "/16"
  }

  staging_cidr = {
    "eu-central-1"   = "/16"
    "us-east-1"      = "/16"
    "ap-southeast-1" = "/16"
  }

  # Those are Raft voters as well. Every other node is a learner.
  bootstrap_irn_nodes = var.environment == "prod" ? {
    # `Node ID -> IP address` mapping

    "eu-central-1a-1" = { ip = "", group_id = 1 },
    "eu-central-1b-1" = { ip = "", group_id = 1 },

    "us-east-1a-1" = { ip = "", group_id = 2 },
    "us-east-1b-1" = { ip = "", group_id = 2 },

    "ap-southeast-1a-1" = { ip = "", group_id = 3 },
    "ap-southeast-1b-1" = { ip = "", group_id = 3 },
  } : local.staging_bootstrap_irn_nodes

  staging_bootstrap_irn_nodes = {
    "eu-central-1a-1" = { ip = "", group_id = 1 },
    "eu-central-1b-1" = { ip = "", group_id = 1 },

    "us-east-1a-1" = { ip = "", group_id = 2 },
    "us-east-1b-1" = { ip = "", group_id = 2 },

    "ap-southeast-1a-1" = { ip = "", group_id = 3 },
    "ap-southeast-1b-1" = { ip = "", group_id = 3 },
  }

  # Bootstrap nodes + regular nodes
  irn_nodes = merge(
    local.bootstrap_irn_nodes, var.environment == "prod" ?
    {
      "eu-central-1c-1" = { ip = "", group_id = 1 },
      "eu-central-1a-2" = { ip = "", group_id = 1 },

      "us-east-1c-1" = { ip = "", group_id = 2 },

      "ap-southeast-1c-1" = { ip = "", group_id = 3 },
      "ap-southeast-1a-2" = { ip = "", group_id = 3 },
      "ap-southeast-1b-2" = { ip = "", group_id = 3 },
  } : local.staging_regular_irn_nodes)

  staging_regular_irn_nodes = {}

  staging_irn_nodes = merge(local.staging_bootstrap_irn_nodes, local.staging_regular_irn_nodes)

  bootstrap_irn_node_peer_ids = [for id, node in local.bootstrap_irn_nodes : "${var.irn_node_keypairs[id].peer_id}_${node.group_id}"]

  # Include only bootnodes in the list of known peers. Once new node joins via a bootnode it will receive
  # all the IPs from the consensus.
  known_peers = { for id, node in local.bootstrap_irn_nodes : "${var.irn_node_keypairs[id].peer_id}_${node.group_id}" => node.ip }

  irn_libp2p_port  = 3000
  irn_metrics_port = 3002
  irn_api_port     = 3003

  irn_node_name = "${var.environment}-irn-node"
  irn_node_tags = {
    Name        = local.irn_node_name
    Group       = "irn"
    Environment = var.environment
  }
}

data "aws_ecr_repository" "irn" {
  name = "irn"
}

data "aws_vpc" "relay" {
  filter {
    name   = "tag:Name"
    values = ["${var.environment}.${var.region}.relay"]
  }
}

data "aws_subnets" "private" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.relay.id]
  }

  filter {
    name   = "tag:Name"
    values = ["${var.environment}.${var.region}.relay-private-${var.region}a"]
  }
}

data "aws_route_tables" "private" {
  vpc_id = data.aws_vpc.relay.id

  filter {
    name   = "tag:Name"
    values = ["${var.environment}.${var.region}.relay-private-${var.region}a"]
  }
}

resource "aws_security_group" "irn_node" {
  name   = local.irn_node_name
  vpc_id = data.aws_vpc.relay.id

  ingress {
    from_port   = local.irn_libp2p_port
    to_port     = local.irn_libp2p_port
    protocol    = "udp"
    cidr_blocks = [for _, v in local.cidr : v]
  }

  ingress {
    from_port   = local.irn_api_port
    to_port     = local.irn_api_port
    protocol    = "udp"
    cidr_blocks = concat([for _, v in local.cidr : v], var.environment == "staging" ? [local.prod_cidr[var.region]] : [])
  }

  # Allow SSH
  ingress {
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = [for _, v in local.cidr : v]
  }

  egress {
    from_port   = 0             # Allowing any incoming port
    to_port     = 0             # Allowing any outgoing port
    protocol    = "-1"          # Allowing any outgoing protocol
    cidr_blocks = ["/0"] # Allowing traffic out to all IP addresses
  }

  tags = local.irn_node_tags
}

module "irn_node" {
  source = "./../irn_node"

  for_each = { for id, addr in local.irn_nodes : id => addr if startswith(id, var.region) }

  region      = var.region
  id          = each.key
  environment = var.environment
  image       = "${data.aws_ecr_repository.irn.repository_url}:${var.irn_node_ecr_image_tag}"
  node_memory = var.irn_node_memory
  node_cpu    = var.irn_node_cpu

  secret_key = var.irn_node_keypairs[each.key].secret_key
  peer_id    = var.irn_node_keypairs[each.key].peer_id
  group_id   = each.value.group_id

  known_peers     = local.known_peers
  bootstrap_nodes = contains(keys(local.bootstrap_irn_nodes), each.key) ? local.bootstrap_irn_node_peer_ids : []
  libp2p_port     = local.irn_libp2p_port
  api_port        = local.irn_api_port
  metrics_port    = local.irn_metrics_port
  log_level       = var.irn_node_log_level

  aws_otel_collector_ecr_repository_url = var.aws_otel_collector_ecr_repository_url
  prometheus_endpoint                   = var.prometheus_endpoint

  vpc_id             = data.aws_vpc.relay.id
  route_table_id     = data.aws_route_tables.private.ids[0]
  security_group_ids = [aws_security_group.irn_node.id]

  ipv4_address                = each.value.ip
  ec2_instance_type           = var.irn_node_ec2_instance_type
  ec2_instance_profile        = var.irn_node_ec2_instance_profile
  ecs_task_execution_role_arn = var.irn_node_ecs_task_execution_role_arn
  ecs_task_role_arn           = var.irn_node_ecs_task_role_arn
  ebs_volume_size             = var.irn_node_ebs_volume_size

  cache_buster = var.irn_cache_buster
}


# For connecting to IRN EC2 instances from AWS console.
resource "aws_ec2_instance_connect_endpoint" "this" {
  subnet_id          = data.aws_subnets.private.ids[0]
  preserve_client_ip = false
}
