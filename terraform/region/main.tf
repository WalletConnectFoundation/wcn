locals {
  # We use different first IP octets for prod/staging in order to use VPC peering between them without collisions.

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

  private_subnets = terraform.workspace == "prod" ? {
    "eu-central-1"   = ["/24", "/24", "/24"]
    "us-east-1"      = ["/24", "/24", "/24"]
    "ap-southeast-1" = ["/24", "/24", "/24"]
    } : {
    "eu-central-1"   = ["/24", "/24", "/24"]
    "us-east-1"      = ["/24", "/24", "/24"]
    "ap-southeast-1" = ["/24", "/24", "/24"]
  }

  public_subnets = terraform.workspace == "prod" ? {
    "eu-central-1"   = ["/24", "/24", "/24"]
    "us-east-1"      = ["/24", "/24", "/24"]
    "ap-southeast-1" = ["/24", "/24", "/24"]
    } : {
    "eu-central-1"   = ["/24", "/24", "/24"]
    "us-east-1"      = ["/24", "/24", "/24"]
    "ap-southeast-1" = ["/24", "/24", "/24"]
  }

  fqdn = (
    terraform.workspace == "prod" ?
    "${var.region}.${var.hosted_zone_name}" :
    "${terraform.workspace}.${var.region}.${var.hosted_zone_name}"
  )
}

module "vpc" {
  source  = "terraform-aws-modules/vpc/aws"
  version = "~> 5.5.0"

  name = "${var.environment}.${var.region}.${var.app_name}"
  cidr = local.cidr[var.region]

  azs                    = ["${var.region}a", "${var.region}b", "${var.region}c"]
  private_subnets        = local.private_subnets[var.region]
  public_subnets         = local.public_subnets[var.region]
  enable_dns_hostnames   = true
  enable_dns_support     = true
  enable_nat_gateway     = true
  single_nat_gateway     = false
  one_nat_gateway_per_az = true

  # New stuff added to the vpc module, which we didn't use before. Setting it up in a way so there are no resource changes.
  manage_default_network_acl    = false
  manage_default_route_table    = false
  manage_default_security_group = false
  map_public_ip_on_launch       = true

  private_subnet_tags = {
    Visibility = "private"
    Class      = "private"
  }
  public_subnet_tags = {
    Visibility = "public"
    Class      = "public"
  }
}

# Query staging VPC in the same region.
data "aws_vpc" "staging" {
  filter {
    name   = "tag:Name"
    values = ["staging.${var.region}.${var.app_name}"]
  }
}

# Do VPC peering between prod and staging in the same region.
#
# Only prod deployments initiate the peering.
resource "aws_vpc_peering_connection" "prod_staging" {
  count = terraform.workspace == "prod" ? 1 : 0

  vpc_id      = module.vpc.vpc_id
  peer_vpc_id = data.aws_vpc.staging.id
  auto_accept = true
}

# Create prod -> staging routes for all private route tables in the same region.
#
# Only prod deployments create the routes.
resource "aws_route" "prod_staging" {
  count = terraform.workspace == "prod" ? length(module.vpc.private_route_table_ids) : 0

  route_table_id            = module.vpc.private_route_table_ids[count.index]
  vpc_peering_connection_id = aws_vpc_peering_connection.prod_staging[0].id
  destination_cidr_block    = local.staging_cidr[var.region]
}

# Query staging route tables in the same region.
data "aws_route_tables" "staging" {
  vpc_id = data.aws_vpc.staging.id

  # All route tables containing "private" in their names
  filter {
    name   = "tag:Name"
    values = ["*private*"]
  }
}

# Create staging -> prod routes for all private route tables in the same region.
#
# Only prod deployments create the routes.
resource "aws_route" "staging_prod" {
  count = terraform.workspace == "prod" ? length(data.aws_route_tables.staging.ids) : 0

  route_table_id            = data.aws_route_tables.staging.ids[count.index]
  vpc_peering_connection_id = aws_vpc_peering_connection.prod_staging[0].id
  destination_cidr_block    = local.cidr[var.region]
}

locals {
  relay_private_subnet_ids = slice(module.vpc.private_subnets, 0, 3)
}

# VPC Endpoints
# Best practice is to keep traffic VPC internal
# as this is more cost-effective
resource "aws_security_group" "vpc-endpoint-group" {
  name        = "${var.environment}.${var.region}.${var.app_name}-vpc-endpoint"
  description = "Allow tls ingress from everywhere"
  vpc_id      = module.vpc.vpc_id
  ingress {
    from_port   = 443
    to_port     = 443
    protocol    = "tcp"
    cidr_blocks = ["/0"]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["/0"]
  }

  tags = {
    Name = "${var.environment}.${var.region}.${var.app_name}-vpc-endpoint"
  }
}

resource "aws_vpc_endpoint" "s3" {
  vpc_id            = module.vpc.vpc_id
  service_name      = "com.amazonaws.${var.region}.s3"
  vpc_endpoint_type = "Gateway"

  route_table_ids = module.vpc.private_route_table_ids
}

resource "aws_vpc_endpoint" "cloudwatch" {
  vpc_id            = module.vpc.vpc_id
  service_name      = "com.amazonaws.${var.region}.logs"
  vpc_endpoint_type = "Interface"

  subnet_ids = local.relay_private_subnet_ids

  security_group_ids = [
    aws_security_group.vpc-endpoint-group.id,
  ]
}

# ECS Task and Schedule for Canary
module "canary" {
  depends_on = [
    module.vpc
  ]

  for_each = terraform.workspace == "dev" ? {} : {
    default = var.canary_project_id,
    irn     = var.canary_project_id_irn,
  }

  source = "./../canary"

  ecr_repository_url = var.sign_client_ecr_repository_url
  app_name           = "${terraform.workspace}_${var.region}_rs-relay_canary_${each.key}"
  region             = var.region
  vpc_id             = module.vpc.vpc_id
  private_subnet_ids = local.relay_private_subnet_ids
  environment        = terraform.workspace
  target_endpoint    = local.fqdn
  tag                = each.key
  canary_project_id  = each.value
  statuspage_api_key = var.statuspage_api_key
  schedule           = "rate(5 minutes)"
}

# ECS Task and Schedule for Canary
module "ui-canary" {
  depends_on = [
    module.vpc
  ]
  count  = terraform.workspace == "dev" ? 0 : 1
  source = "./../canary"

  ecr_repository_url = var.web3modal_canary_ecr_repository_url
  app_name           = "${terraform.workspace}_${var.region}_rs-relay_ui_canary"
  region             = var.region
  vpc_id             = module.vpc.vpc_id
  private_subnet_ids = local.relay_private_subnet_ids
  environment        = terraform.workspace
  target_endpoint    = local.fqdn
  canary_project_id  = var.canary_project_id
  ecr_image_tag      = "V4"
  cpu                = 1024
  statuspage_api_key = var.statuspage_api_key
  schedule           = "rate(15 minutes)"
  command            = "[\"npm\", \"run\", \"playwright:test:canary\"]"
}

# Service
module "dns" {
  source = "github.com/WalletConnect/terraform-modules/modules/dns"

  hosted_zone_name = var.hosted_zone_name
  fqdn             = local.fqdn
}

module "ecs" {
  depends_on = [
    module.vpc
  ]
  source = "./../ecs"

  ecr_repository_url                    = var.ecr_repository_url
  ecr_app_version                       = var.ecr_app_version
  aws_otel_collector_ecr_repository_url = var.aws_otel_collector_ecr_repository_url
  app_name                              = "${var.environment}_${var.region}_${var.app_name}"
  region                                = var.region
  vpc_id                                = module.vpc.vpc_id
  vpc_cidr_blocks                       = values(local.cidr)
  private_subnet_ids                    = local.relay_private_subnet_ids
  public_subnet_ids                     = module.vpc.public_subnets
  public_port                           = 8080
  private_port                          = 8081
  environment                           = var.environment
  prometheus_endpoint                   = var.prometheus_endpoint
  acm_certificate_arn                   = module.dns.certificate_arn
  fqdn                                  = local.fqdn
  auth_fqdns                            = var.auth_fqdns
  route53_zone_id                       = module.dns.zone_id
  autoscaling_max_capacity              = var.autoscaling_max_capacity
  autoscaling_min_capacity              = var.autoscaling_min_capacity
  analytics-data-lake_bucket_name       = var.analytics_data_lake_bucket_name
  analytics_data_lake-bucket-key_arn    = var.analytics_data_lake_bucket_key_arn
  disconnect_timeout_ms                 = var.disconnect_timeout_ms
  heartbeat_interval_ms                 = var.heartbeat_interval_ms
  heartbeat_timeout_ms                  = var.heartbeat_timeout_ms
  storage_replication_lag_ms            = var.storage_replication_lag_ms
  log_level                             = var.relay_log_level
  telemetry_level                       = var.relay_telemetry_level
  telemetry_sample_ratio                = var.relay_telemetry_sample_ratio
  relay_node_cpu                        = var.relay_node_cpu
  relay_node_memory                     = var.relay_node_memory
  lb_count                              = var.relay_lb_count
  lb_deregistration_delay_secs          = var.relay_lb_deregistration_delay_secs

  analytics_geoip_db_bucket_name = var.analytics_geoip_db_bucket_name
  analytics_geoip_db_key         = var.analytics_geoip_db_key
  project_data_cache_ttl         = var.project_data_cache_ttl
  registry_api_endpoint          = var.registry_api_endpoint
  registry_api_auth_token        = var.registry_api_auth_token


  webhook_amqp_addr_primary   = var.webhook_amqp_addr_primary
  webhook_amqp_addr_secondary = var.webhook_amqp_addr_secondary

  ed25519_private_key  = var.ed25519_private_key
  irn_namespace_secret = var.irn_namespace_secret

  ofac_countries           = var.ofac_countries
  rpc_rate_limit_whitelist = var.rpc_rate_limit_whitelist

  # List of IRN nodes and their IP adresses to be used by relay via new client API.
  irn_nodes           = { for id, node in local.irn_nodes : var.irn_node_keypairs[id].peer_id => node.ip if startswith(id, var.region) }
  irn_shadowing_nodes = { for id, node in local.staging_irn_nodes : var.irn_node_keypairs[id].staging_peer_id => node.ip if startswith(id, var.region) }
  irn_api_port        = local.irn_api_port

  cache_buster = var.relay_cache_buster
}

module "tgw" {
  source = "terraform-aws-modules/transit-gateway/aws"

  name        = "${var.environment}_${var.region}_${var.app_name}"
  description = "${var.environment}_${var.region}_${var.app_name} Transit Gateway"

  ram_allow_external_principals = false
  share_tgw                     = false

  enable_auto_accept_shared_attachments  = true
  enable_default_route_table_association = false
  enable_default_route_table_propagation = false

  vpc_attachments = {
    vpc1 = {
      vpc_id      = module.vpc.vpc_id
      subnet_ids  = local.relay_private_subnet_ids
      dns_support = true

      transit_gateway_default_route_table_association = false
      transit_gateway_default_route_table_propagation = false

      tgw_routes = [
        {
          destination_cidr_block = module.vpc.vpc_cidr_block
        }
      ]
    },
  }

  tags = {
    Name        = "${var.environment}.${var.region}.${var.app_name}-tgw"
    Application = "relay"
  }
}

resource "aws_route53_health_check" "health_check" {
  fqdn              = local.fqdn
  port              = 443
  type              = "HTTPS"
  resource_path     = "/health"
  failure_threshold = "5"
  request_interval  = "30"

  tags = {
    Name = "${var.environment}.${var.region}.${var.app_name}-health-check"
  }
}

##### IRN resources

locals {
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

resource "aws_security_group" "irn_node" {
  name   = local.irn_node_name
  vpc_id = module.vpc.vpc_id

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
  depends_on = [module.vpc]
  source     = "./../irn_node"

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

  vpc_id             = module.vpc.vpc_id
  route_table_id     = module.vpc.private_route_table_ids[0]
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
  subnet_id          = module.vpc.private_subnets[0]
  preserve_client_ip = false
}
