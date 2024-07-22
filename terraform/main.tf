terraform {
  required_version = ">= 1.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = ">= 5.0"
    }
    libp2p = {
      source  = "WalletConnect/libp2p"
      version = "~> 0.1.0"
    }
  }

  backend "remote" {
    hostname     = "app.terraform.io"
    organization = "wallet-connect"
    workspaces {
      name = "irn-testnet"
    }
  }
}

locals {
  environment = "testnet"
  tags = {
    Application = "irn"
    Group       = "irn"
    Environment = local.environment
  }

  admin_peer_id = "12D3KooWBumV8hAjhXJpV84H6KF4ei7RBxcjU1oA6J5t91Jvchuq"
}

provider "aws" {
  region = "eu-central-1"

  default_tags {
    tags = local.tags
  }
}

variable "eth_rpc_url" {
  type      = string
  sensitive = true
}

variable "smart_contract_signer_mnemonic" {
  type      = string
  sensitive = true
}

resource "aws_vpc" "this" {
  cidr_block = "/16"
}

resource "aws_route_table" "public" {
  vpc_id = aws_vpc.this.id
}

resource "aws_internet_gateway" "this" {
  vpc_id = aws_vpc.this.id
}

resource "aws_route" "internet_gateway" {
  route_table_id         = aws_route_table.public.id
  destination_cidr_block = "/0"
  gateway_id             = aws_internet_gateway.this.id
}

locals {
  bootstrap_nodes = {
    "eu-central-1a-1" = { ip = "", group_id = 1 },
    "eu-central-1b-1" = { ip = "", group_id = 2 },
    "eu-central-1c-1" = { ip = "", group_id = 3 },
  }

  operator_nodes = {
    "eu-central-1a-2" = { ip = "", group_id = 1 },
  }

  nodes = merge(local.bootstrap_nodes, local.operator_nodes)

  bootstrap_node_ids = [for id, node in local.bootstrap_nodes : "${module.keypair[id].peer_id}_${node.group_id}"]
  bootstrap_peer_ids = [for id, node in local.bootstrap_nodes : module.keypair[id].peer_id]

  operator_peer_ids = concat([for id, node in local.operator_nodes : module.keypair[id].peer_id], [
    "12D3KooWKNoDLQWimQ3zJTmKkEeezCBrjZTw6Tgu4UZEGTjWEJ65", # consensys
    "12D3KooWC6xCiL7WXZc4RqiLqDYAythsrjKY1i2qiqYaYoL2XHvu", # luga
    "12D3KooWPkasjzJTX7uTcxZjgzihQ7fheYNay7bMDQvZvmKuFrWw", # 1kx
    "12D3KooWHHdsq8TMRkb22seAWDTpQMDPNAx7a4yyUhrt3WHzwqM2", # ledger old
    "12D3KooWE5giMrZBa2jag1MCkNb8KXxQwcnzALZDNeiXSeHePVWP", # ledger new
    "12D3KooWF4jm2nrMaoJwyaNd3y8dvvVayXSHZdwpww6LvJr4ekC9", # kiln

    "12D3KooWPbKnCbBSp7znwgAirAyPiZd3wwzrSeeUEuTH9YEFxQP4", # wc Chris
  ])

  known_peers = { for id, node in local.bootstrap_nodes : "${module.keypair[id].peer_id}_${node.group_id}" => aws_eip.this[id].public_ip }

  port         = 3000
  metrics_port = 3002
  api_port     = 3003

  grafana_port = 9091
}

resource "aws_eip" "this" {
  for_each = local.nodes
}

module "keypair" {
  source = "./keypair"

  for_each      = local.nodes
  node_id       = each.key
  environment   = local.environment
  query_staging = false

  tags = local.tags
}

# Ed25519 secret key, 32 bytes
resource "random_bytes" "admin_secret_key" {
  length = 32
}

resource "aws_secretsmanager_secret" "admin_secret_key" {
  name = "testnet_admin_secret_key"

  # Otherwise Terraform won't be able to delete it
  recovery_window_in_days = 0

  tags = local.tags
}

resource "aws_secretsmanager_secret_version" "admin_secret_key" {
  secret_id     = aws_secretsmanager_secret.admin_secret_key.id
  secret_string = random_bytes.admin_secret_key.base64
}

data "libp2p_peer_id" "admin_peer_id" {
  ed25519_secret_key = aws_secretsmanager_secret_version.admin_secret_key.secret_string
}

resource "aws_prometheus_workspace" "this" {
  alias = "prometheus-irn-testnet"
}

data "aws_ecr_repository" "node" {
  name = "irn"
}

resource "aws_security_group" "node" {
  name   = "irn-node"
  vpc_id = aws_vpc.this.id

  ingress {
    from_port   = local.port
    to_port     = local.port
    protocol    = "udp"
    cidr_blocks = ["/0"]
  }

  ingress {
    from_port   = local.api_port
    to_port     = local.api_port
    protocol    = "udp"
    cidr_blocks = ["/0"]
  }

  # Prometheus
  ingress {
    from_port   = 9090
    to_port     = 9090
    protocol    = "tcp"
    cidr_blocks = ["/0"]
  }

  # Grafana
  ingress {
    from_port   = local.grafana_port
    to_port     = local.grafana_port
    protocol    = "tcp"
    cidr_blocks = ["/0"]
  }

  # Allow SSH
  ingress {
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = [aws_vpc.this.cidr_block]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["/0"]
  }

  tags = local.tags
}

module "node" {
  source = "./node"

  for_each = local.nodes

  region      = "eu-central-1"
  id          = each.key
  environment = local.environment
  image       = "${data.aws_ecr_repository.node.repository_url}:1.432.2"
  node_memory = 4096 - 512
  node_cpu    = 2048

  secret_key = module.keypair[each.key].secret_key
  peer_id    = module.keypair[each.key].peer_id
  group_id   = each.value.group_id

  known_peers     = local.known_peers
  bootstrap_nodes = contains(keys(local.bootstrap_nodes), each.key) ? local.bootstrap_node_ids : []
  libp2p_port     = local.port
  api_port        = local.api_port
  metrics_port    = local.metrics_port
  log_level       = "INFO"

  prometheus_endpoint = aws_prometheus_workspace.this.prometheus_endpoint
  # One of our nodes monitors the whole cluster
  prometheus_target_peer_ids = each.key == "eu-central-1a-1" ? concat(local.bootstrap_peer_ids, local.operator_peer_ids) : [module.keypair[each.key].peer_id]

  vpc_id             = aws_vpc.this.id
  route_table_id     = aws_route_table.public.id
  security_group_ids = [aws_security_group.node.id]

  ipv4_address     = each.value.ip
  expose_public_ip = true
  eip_id           = aws_eip.this[each.key].id

  ec2_instance_type           = "c5a.large"
  ec2_instance_profile        = aws_iam_instance_profile.ec2_instance_profile.name
  ecs_task_execution_role_arn = aws_iam_role.ecs_task_execution_role.arn
  ecs_task_role_arn           = aws_iam_role.ecs_task_execution_role.arn
  ebs_volume_size             = 10

  tags = local.tags

  authorized_raft_candidates = concat([local.admin_peer_id], local.operator_peer_ids)
  authorized_clients         = [local.admin_peer_id]

  enable_otel_collector     = false
  enable_prometheus         = true
  prometheus_admin_password = aws_secretsmanager_secret_version.admin_secret_key.secret_string
  enable_grafana            = true
  grafana_port              = local.grafana_port
  grafana_admin_password    = aws_secretsmanager_secret_version.admin_secret_key.secret_string

  config_smart_contract_address = "0xe6eE5164fe97f7a779aea4251148E106D4bC962E"
  eth_rpc_url                   = var.eth_rpc_url
  eth_address                   = contains(keys(local.operator_nodes), each.key) ? "0xE8A15C006D14Cf8d70391933F524ceaade185b3f" : null

  # Only a single node has write access to the contract
  smart_contract_signer_mnemonic = each.key == "eu-central-1a-1" ? var.smart_contract_signer_mnemonic : null
}

data "aws_iam_policy_document" "assume_role" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["ecs-tasks.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "ecs_task_execution_role" {
  name               = "irn_node_ecs_task_execution_role"
  assume_role_policy = data.aws_iam_policy_document.assume_role.json
}

resource "aws_iam_role_policy_attachment" "ecs_task_execution_role_policy" {
  role       = aws_iam_role.ecs_task_execution_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
}

resource "aws_iam_role_policy_attachment" "cloudwatch_write_policy" {
  role       = aws_iam_role.ecs_task_execution_role.name
  policy_arn = "arn:aws:iam::aws:policy/CloudWatchLogsFullAccess"
}

resource "aws_iam_role_policy_attachment" "prometheus_write_policy" {
  role       = aws_iam_role.ecs_task_execution_role.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonPrometheusRemoteWriteAccess"
}

data "aws_iam_policy_document" "ec2_instance_policy" {
  statement {
    principals {
      type        = "Service"
      identifiers = ["ec2.amazonaws.com"]
    }

    actions = ["sts:AssumeRole"]
  }
}

resource "aws_iam_role" "ec2_instance_role" {
  name               = "irn_node_ec2_instance_role"
  assume_role_policy = data.aws_iam_policy_document.ec2_instance_policy.json
}

resource "aws_iam_role_policy_attachment" "ec2_instance_role_attachment" {
  role       = aws_iam_role.ec2_instance_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonEC2ContainerServiceforEC2Role"
}

resource "aws_iam_role_policy_attachment" "ec2_instance_connect_policy" {
  role       = aws_iam_role.ec2_instance_role.name
  policy_arn = "arn:aws:iam::aws:policy/EC2InstanceConnect"
}

resource "aws_iam_instance_profile" "ec2_instance_profile" {
  name = "irn_node_ec2_instance_profile"
  role = aws_iam_role.ec2_instance_role.name
}

# For connecting to IRN EC2 instances from AWS console.
resource "aws_ec2_instance_connect_endpoint" "this" {
  subnet_id          = module.node["eu-central-1a-1"].subnet_id
  preserve_client_ip = false
}
