locals {
  app_name = "relay"

  geo_ip_database_bucket = module.geo_ip_database_bucket.bucket # TODO: switch to var when the geoip bucket is migrated.

  hosted_zone_name = "relay.walletconnect.com"

  fqdn = (
    terraform.workspace == "prod" ?
    local.hosted_zone_name :
    "${terraform.workspace}.${local.hosted_zone_name}"
  )

  ap_southeast_1_extra_domains = [
    {
      hosted_zone_name = "relay.walletconnect.org"
      fqdn             = terraform.workspace == "prod" ? "relay.walletconnect.org" : "${terraform.workspace}.relay.walletconnect.org"
    }
  ]

  relay_auth_fqdns = concat(
    [local.fqdn, module.eu-central-1.fqdn, module.us-east-1.fqdn, module.ap-southeast-1.fqdn],
    module.eu-central-1.lb_fqdns,
    module.us-east-1.lb_fqdns,
    module.ap-southeast-1.lb_fqdns,
    local.ap_southeast_1_extra_domains[*].fqdn
  )

  environment = terraform.workspace

  irn_namespace_secret = data.aws_secretsmanager_secret_version.irn_namespace_secret.secret_string
}

data "assert_test" "workspace" {
  test  = terraform.workspace != "default"
  throw = "default workspace is not valid in this project"
}

module "tags" {
  source = "github.com/WalletConnect/terraform-modules/modules/tags"

  application = local.app_name
  env         = terraform.workspace

  providers = {
    aws = aws.eu-central-1
  }
}

data "aws_ecr_repository" "relay" {
  name = local.app_name
}

data "aws_ecr_repository" "sign_client" {
  name = "sign-client"
}

data "aws_ecr_repository" "web3modal_canary" {
  name = "web3modal-canary"
}

data "aws_ecr_repository" "auth_client" {
  name = "auth-client"
}

data "aws_ecr_repository" "chat_client" {
  name = "chat-client"
}

data "aws_ecr_repository" "aws_otel_collector" {
  name = "aws-otel-collector"
}

data "aws_secretsmanager_secret" "irn_namespace_secret" {
  arn = aws_secretsmanager_secret.irn_namespace_secret.arn
}

data "aws_secretsmanager_secret_version" "irn_namespace_secret" {
  secret_id = data.aws_secretsmanager_secret.irn_namespace_secret.arn
}

resource "random_password" "irn_namespace_secret" {
  length  = 32
  special = false
}

resource "aws_secretsmanager_secret" "irn_namespace_secret" {
  name = "${local.environment}-${local.app_name}-storage-namespace-secret"
}

resource "aws_secretsmanager_secret_version" "irn_namespace_secret" {
  secret_id     = aws_secretsmanager_secret.irn_namespace_secret.id
  secret_string = random_password.irn_namespace_secret.result
}

# ECS Task and Schedule for Canary
module "canary" {
  for_each = terraform.workspace == "dev" ? {} : {
    default = var.canary_project_id,
    irn     = var.canary_project_id_irn,
  }

  source = "./canary"

  ecr_repository_url = data.aws_ecr_repository.sign_client.repository_url
  app_name           = "${terraform.workspace}_rs-relay_canary_${each.key}"
  region             = var.region
  vpc_id             = module.eu-central-1.vpc_id
  private_subnet_ids = module.eu-central-1.private_subnets
  environment        = terraform.workspace
  target_endpoint    = local.fqdn
  tag                = each.key
  canary_project_id  = each.value
  statuspage_api_key = var.statuspage_api_key

  providers = {
    aws = aws.eu-central-1
  }
}

# ECS Task and Schedule for Canary
module "auth_canary" {
  count  = terraform.workspace == "dev" ? 0 : 1
  source = "./canary"

  ecr_repository_url = data.aws_ecr_repository.auth_client.repository_url
  app_name           = "${terraform.workspace}_rs-relay_canary_auth"
  region             = var.region
  vpc_id             = module.eu-central-1.vpc_id
  private_subnet_ids = module.eu-central-1.private_subnets
  environment        = terraform.workspace
  target_endpoint    = local.fqdn
  canary_project_id  = var.canary_project_id
  statuspage_api_key = var.statuspage_api_key

  providers = {
    aws = aws.eu-central-1
  }
}

# ECS Task and Schedule for Canary
module "chat_canary" {
  count  = terraform.workspace == "dev" ? 0 : 1
  source = "./canary"

  ecr_repository_url = data.aws_ecr_repository.chat_client.repository_url
  app_name           = "${terraform.workspace}_rs-relay_canary_chat"
  region             = var.region
  vpc_id             = module.eu-central-1.vpc_id
  private_subnet_ids = module.eu-central-1.private_subnets
  environment        = terraform.workspace
  target_endpoint    = local.fqdn
  canary_project_id  = var.canary_project_id
  statuspage_api_key = var.statuspage_api_key

  providers = {
    aws = aws.eu-central-1
  }
}

# ECS Load Test
module "load_test" {
  source = "./load_test"

  ecr_repository_url   = data.aws_ecr_repository.sign_client.repository_url
  app_name             = "${terraform.workspace}_rs-relay_load_test"
  region               = var.region
  vpc_id               = module.eu-central-1.vpc_id
  environment          = terraform.workspace
  target_endpoint      = local.fqdn
  load_test_project_id = var.canary_project_id

  providers = {
    aws = aws.eu-central-1
  }
}

resource "aws_prometheus_workspace" "prometheus" {
  alias = "prometheus-${terraform.workspace}-${local.app_name}"
}

module "monitoring" {
  source = "./monitoring"

  target_groups = {
    "eu-central-1"   = module.eu-central-1.lb_target_group_arns
    "us-east-1"      = module.us-east-1.lb_target_group_arns
    "ap-southeast-1" = module.ap-southeast-1.lb_target_group_arns
  }
  load_balancers = {
    "eu-central-1"   = module.eu-central-1.lb_arns
    "us-east-1"      = module.us-east-1.lb_arns
    "ap-southeast-1" = module.ap-southeast-1.lb_arns
  }

  environment             = terraform.workspace
  fqdn                    = local.fqdn
  regionalized_fqdns      = [module.eu-central-1.fqdn, module.us-east-1.fqdn, module.ap-southeast-1.fqdn]
  prometheus_workspace_id = aws_prometheus_workspace.prometheus.id
  ecs_service_names       = [module.eu-central-1.ecs_service_name, module.us-east-1.ecs_service_name, module.ap-southeast-1.ecs_service_name]
  irn_nodes               = [for _, node in merge(module.eu-central-1.irn_nodes, module.us-east-1.irn_nodes, module.ap-southeast-1.irn_nodes) : node]

  providers = {
    aws = aws.eu-central-1
  }
}

module "webhook-activemq" {
  source = "./activemq"

  app_name               = local.app_name
  broker_name            = "webhook-activemq"
  environment            = terraform.workspace
  primary_instance_class = var.webhook_activemq_primary_instance_class
  region                 = "eu-central-1"

  allowed_ingress_cidr_blocks = [
    module.eu-central-1.cidr_block,
    module.us-east-1.cidr_block,
    module.ap-southeast-1.cidr_block
  ]

  vpc_id             = module.eu-central-1.vpc_id
  private_subnet_ids = module.eu-central-1.private_subnets

  providers = {
    aws = aws.eu-central-1
  }
}

module "eu-central-1" {
  source                                = "./region"
  environment                           = terraform.workspace
  region                                = "eu-central-1"
  app_name                              = local.app_name
  hosted_zone_name                      = local.hosted_zone_name
  sign_client_ecr_repository_url        = data.aws_ecr_repository.sign_client.repository_url
  web3modal_canary_ecr_repository_url   = data.aws_ecr_repository.web3modal_canary.repository_url
  ecr_repository_url                    = data.aws_ecr_repository.relay.repository_url
  aws_otel_collector_ecr_repository_url = data.aws_ecr_repository.aws_otel_collector.repository_url
  ecr_app_version                       = var.ecr_app_version
  canary_project_id                     = var.canary_project_id
  canary_project_id_irn                 = var.canary_project_id_irn
  statuspage_api_key                    = var.statuspage_api_key
  disconnect_timeout_ms                 = var.disconnect_timeout_ms
  heartbeat_interval_ms                 = var.heartbeat_interval_ms
  heartbeat_timeout_ms                  = var.heartbeat_timeout_ms
  storage_replication_lag_ms            = var.storage_replication_lag_eu_ms
  auth_fqdns                            = local.relay_auth_fqdns
  relay_log_level                       = var.relay_log_level
  relay_telemetry_level                 = var.relay_telemetry_level
  relay_telemetry_sample_ratio          = var.relay_telemetry_sample_ratio
  relay_node_cpu                        = var.relay_node_cpu
  relay_node_memory                     = var.relay_node_memory
  relay_lb_count                        = var.relay_lb_count
  relay_lb_deregistration_delay_secs    = var.relay_lb_deregistration_delay_secs
  irn_node_cpu                          = var.irn_node_cpu
  irn_node_memory                       = var.irn_node_memory
  irn_node_log_level                    = var.irn_node_log_level
  irn_cache_buster                      = var.irn_cache_buster
  irn_namespace_secret                  = local.irn_namespace_secret
  relay_cache_buster                    = var.relay_cache_buster

  prometheus_endpoint      = aws_prometheus_workspace.prometheus.prometheus_endpoint
  autoscaling_max_capacity = var.autoscaling_max_instances
  autoscaling_min_capacity = var.autoscaling_min_instances
  project_data_cache_ttl   = var.project_data_cache_ttl
  registry_api_endpoint    = var.registry_api_endpoint
  registry_api_auth_token  = var.registry_api_auth_token

  webhook_amqp_addr_primary   = module.webhook-activemq.primary_connection_url
  webhook_amqp_addr_secondary = module.webhook-activemq.secondary_connection_url

  analytics_data_lake_bucket_name    = var.data_lake_bucket_name
  analytics_data_lake_bucket_key_arn = var.data_lake_bucket_key_arn
  analytics_geoip_db_bucket_name     = local.geo_ip_database_bucket
  analytics_geoip_db_key             = var.geoip_db_key

  ed25519_private_key = var.ed25519_private_key

  ofac_countries           = var.ofac_countries
  rpc_rate_limit_whitelist = var.rpc_rate_limit_whitelist

  providers = {
    aws = aws.eu-central-1
  }

  irn_node_keypairs                    = module.irn_node_keypair
  irn_node_ecr_image_tag               = var.irn_node_ecr_image_tag
  irn_node_ec2_instance_type           = var.irn_node_ec2_instance_type
  irn_node_ec2_instance_profile        = aws_iam_instance_profile.irn_node_ec2_instance_profile.name
  irn_node_ecs_task_execution_role_arn = aws_iam_role.irn_node_ecs_task_execution_role.arn
  irn_node_ecs_task_role_arn           = aws_iam_role.irn_node_ecs_task_execution_role.arn
  irn_node_ebs_volume_size             = var.irn_node_ebs_volume_size
}

module "us-east-1" {
  source                                = "./region"
  environment                           = terraform.workspace
  region                                = "us-east-1"
  app_name                              = local.app_name
  hosted_zone_name                      = local.hosted_zone_name
  sign_client_ecr_repository_url        = replace(data.aws_ecr_repository.sign_client.repository_url, "eu-central-1", "us-east-1")
  web3modal_canary_ecr_repository_url   = replace(data.aws_ecr_repository.web3modal_canary.repository_url, "eu-central-1", "us-east-1")
  ecr_repository_url                    = replace(data.aws_ecr_repository.relay.repository_url, "eu-central-1", "us-east-1")
  aws_otel_collector_ecr_repository_url = replace(data.aws_ecr_repository.aws_otel_collector.repository_url, "eu-central-1", "us-east-1")
  ecr_app_version                       = var.ecr_app_version
  canary_project_id                     = var.canary_project_id
  canary_project_id_irn                 = var.canary_project_id_irn
  statuspage_api_key                    = var.statuspage_api_key
  disconnect_timeout_ms                 = var.disconnect_timeout_ms
  heartbeat_interval_ms                 = var.heartbeat_interval_ms
  heartbeat_timeout_ms                  = var.heartbeat_timeout_ms
  storage_replication_lag_ms            = var.storage_replication_lag_us_ms
  auth_fqdns                            = local.relay_auth_fqdns
  relay_log_level                       = var.relay_log_level
  relay_telemetry_level                 = var.relay_telemetry_level
  relay_telemetry_sample_ratio          = var.relay_telemetry_sample_ratio
  relay_node_cpu                        = var.relay_node_cpu
  relay_node_memory                     = var.relay_node_memory
  relay_lb_count                        = var.relay_lb_count
  relay_lb_deregistration_delay_secs    = var.relay_lb_deregistration_delay_secs
  irn_node_cpu                          = var.irn_node_cpu
  irn_node_memory                       = var.irn_node_memory
  irn_node_log_level                    = var.irn_node_log_level
  irn_cache_buster                      = var.irn_cache_buster
  relay_cache_buster                    = var.relay_cache_buster
  irn_namespace_secret                  = local.irn_namespace_secret

  prometheus_endpoint      = aws_prometheus_workspace.prometheus.prometheus_endpoint
  autoscaling_max_capacity = var.autoscaling_max_instances
  autoscaling_min_capacity = var.autoscaling_min_instances
  project_data_cache_ttl   = var.project_data_cache_ttl
  registry_api_endpoint    = var.registry_api_endpoint
  registry_api_auth_token  = var.registry_api_auth_token

  webhook_amqp_addr_primary   = module.webhook-activemq.primary_connection_url
  webhook_amqp_addr_secondary = module.webhook-activemq.secondary_connection_url

  analytics_data_lake_bucket_name    = var.data_lake_bucket_name
  analytics_data_lake_bucket_key_arn = var.data_lake_bucket_key_arn
  analytics_geoip_db_bucket_name     = local.geo_ip_database_bucket
  analytics_geoip_db_key             = var.geoip_db_key

  ed25519_private_key = var.ed25519_private_key

  ofac_countries           = var.ofac_countries
  rpc_rate_limit_whitelist = var.rpc_rate_limit_whitelist

  providers = {
    aws = aws.us-east-1
  }

  irn_node_keypairs                    = module.irn_node_keypair
  irn_node_ecr_image_tag               = var.irn_node_ecr_image_tag
  irn_node_ec2_instance_type           = var.irn_node_ec2_instance_type
  irn_node_ec2_instance_profile        = aws_iam_instance_profile.irn_node_ec2_instance_profile.name
  irn_node_ecs_task_execution_role_arn = aws_iam_role.irn_node_ecs_task_execution_role.arn
  irn_node_ecs_task_role_arn           = aws_iam_role.irn_node_ecs_task_execution_role.arn
  irn_node_ebs_volume_size             = var.irn_node_ebs_volume_size
}

module "ap-southeast-1" {
  source                                = "./region"
  environment                           = terraform.workspace
  region                                = "ap-southeast-1"
  app_name                              = local.app_name
  hosted_zone_name                      = local.hosted_zone_name
  sign_client_ecr_repository_url        = replace(data.aws_ecr_repository.sign_client.repository_url, "eu-central-1", "ap-southeast-1")
  web3modal_canary_ecr_repository_url   = replace(data.aws_ecr_repository.web3modal_canary.repository_url, "eu-central-1", "ap-southeast-1")
  ecr_repository_url                    = replace(data.aws_ecr_repository.relay.repository_url, "eu-central-1", "ap-southeast-1")
  aws_otel_collector_ecr_repository_url = replace(data.aws_ecr_repository.aws_otel_collector.repository_url, "eu-central-1", "ap-southeast-1")
  ecr_app_version                       = var.ecr_app_version
  canary_project_id                     = var.canary_project_id
  canary_project_id_irn                 = var.canary_project_id_irn
  statuspage_api_key                    = var.statuspage_api_key
  disconnect_timeout_ms                 = var.disconnect_timeout_ms
  heartbeat_interval_ms                 = var.heartbeat_interval_ms
  heartbeat_timeout_ms                  = var.heartbeat_timeout_ms
  storage_replication_lag_ms            = var.storage_replication_lag_ap_ms
  auth_fqdns                            = local.relay_auth_fqdns
  relay_log_level                       = var.relay_log_level
  relay_telemetry_level                 = var.relay_telemetry_level
  relay_telemetry_sample_ratio          = var.relay_telemetry_sample_ratio
  relay_node_cpu                        = var.relay_node_cpu
  relay_node_memory                     = var.relay_node_memory
  relay_lb_count                        = var.relay_lb_count
  relay_lb_deregistration_delay_secs    = var.relay_lb_deregistration_delay_secs
  irn_node_cpu                          = var.irn_node_cpu
  irn_node_memory                       = var.irn_node_memory
  irn_node_log_level                    = var.irn_node_log_level
  irn_cache_buster                      = var.irn_cache_buster
  relay_cache_buster                    = var.relay_cache_buster
  irn_namespace_secret                  = local.irn_namespace_secret

  prometheus_endpoint      = aws_prometheus_workspace.prometheus.prometheus_endpoint
  autoscaling_max_capacity = var.autoscaling_max_instances
  autoscaling_min_capacity = var.autoscaling_min_instances
  project_data_cache_ttl   = var.project_data_cache_ttl
  registry_api_endpoint    = var.registry_api_endpoint
  registry_api_auth_token  = var.registry_api_auth_token

  webhook_amqp_addr_primary   = module.webhook-activemq.primary_connection_url
  webhook_amqp_addr_secondary = module.webhook-activemq.secondary_connection_url

  analytics_data_lake_bucket_name    = var.data_lake_bucket_name
  analytics_data_lake_bucket_key_arn = var.data_lake_bucket_key_arn
  analytics_geoip_db_bucket_name     = local.geo_ip_database_bucket
  analytics_geoip_db_key             = var.geoip_db_key

  ed25519_private_key = var.ed25519_private_key

  ofac_countries           = var.ofac_countries
  rpc_rate_limit_whitelist = var.rpc_rate_limit_whitelist

  providers = {
    aws = aws.ap-southeast-1
  }

  irn_node_keypairs                    = module.irn_node_keypair
  irn_node_ecr_image_tag               = var.irn_node_ecr_image_tag
  irn_node_ec2_instance_type           = var.irn_node_ec2_instance_type
  irn_node_ec2_instance_profile        = aws_iam_instance_profile.irn_node_ec2_instance_profile.name
  irn_node_ecs_task_execution_role_arn = aws_iam_role.irn_node_ecs_task_execution_role.arn
  irn_node_ecs_task_role_arn           = aws_iam_role.irn_node_ecs_task_execution_role.arn
  irn_node_ebs_volume_size             = var.irn_node_ebs_volume_size
}

# DNS Records
module "dns" {
  source = "github.com/WalletConnect/terraform-modules/modules/dns"

  hosted_zone_name = local.hosted_zone_name
  fqdn             = local.fqdn

  providers = {
    aws = aws.eu-central-1
  }
}

module "dns_us" {
  source = "github.com/WalletConnect/terraform-modules/modules/dns"

  hosted_zone_name = local.hosted_zone_name
  fqdn             = local.fqdn

  providers = {
    aws = aws.us-east-1
  }
}

module "dns_ap" {
  source = "github.com/WalletConnect/terraform-modules/modules/dns"

  hosted_zone_name = local.hosted_zone_name
  fqdn             = local.fqdn

  providers = {
    aws = aws.ap-southeast-1
  }
}

resource "aws_route53_record" "relay_record_eu" {
  zone_id = module.dns.zone_id
  name    = local.fqdn
  type    = "A"

  set_identifier = "eu"

  health_check_id = module.eu-central-1.health_check_id

  alias {
    name                   = module.eu-central-1.fqdn
    zone_id                = module.dns.zone_id
    evaluate_target_health = true
  }

  latency_routing_policy {
    region = "eu-central-1"
  }
}

resource "aws_route53_record" "relay_record_us" {
  depends_on = [
    aws_route53_record.relay_record_eu
  ]

  zone_id = module.dns_us.zone_id
  name    = local.fqdn
  type    = "A"

  set_identifier = "us"

  health_check_id = module.us-east-1.health_check_id

  alias {
    name                   = module.us-east-1.fqdn
    zone_id                = module.dns_us.zone_id
    evaluate_target_health = true
  }

  latency_routing_policy {
    region = "us-east-1"
  }
}

resource "aws_route53_record" "relay_record_ap" {
  depends_on = [
    aws_route53_record.relay_record_eu
  ]

  zone_id = module.dns_ap.zone_id
  name    = local.fqdn
  type    = "A"

  set_identifier = "ap"

  health_check_id = module.ap-southeast-1.health_check_id

  alias {
    name                   = module.ap-southeast-1.fqdn
    zone_id                = module.dns_ap.zone_id
    evaluate_target_health = true
  }

  latency_routing_policy {
    region = "ap-southeast-1"
  }
}

resource "aws_lb_listener_certificate" "relay_record_cert_eu" {
  count           = length(module.eu-central-1.lb_listener_arns)
  listener_arn    = module.eu-central-1.lb_listener_arns[count.index]
  certificate_arn = module.dns.certificate_arn

  provider = aws.eu-central-1
}

resource "aws_lb_listener_certificate" "relay_record_cert_us" {
  count           = length(module.us-east-1.lb_listener_arns)
  listener_arn    = module.us-east-1.lb_listener_arns[count.index]
  certificate_arn = module.dns_us.certificate_arn

  provider = aws.us-east-1
}

resource "aws_lb_listener_certificate" "relay_record_cert_ap" {
  count           = length(module.ap-southeast-1.lb_listener_arns)
  listener_arn    = module.ap-southeast-1.lb_listener_arns[count.index]
  certificate_arn = module.dns_ap.certificate_arn

  provider = aws.ap-southeast-1
}

############################
#
# Peering accounts requires manual interaction when setting up initially
# See https://docs.aws.amazon.com/vpc/latest/tgw/tgw-peering.html
#
############################

data "aws_caller_identity" "current" {}

# EU -> US
resource "aws_ec2_transit_gateway_peering_attachment" "eu-us-peering" {
  peer_region             = "us-east-1"
  peer_transit_gateway_id = module.us-east-1.transit_gateway_id
  transit_gateway_id      = module.eu-central-1.transit_gateway_id
  peer_account_id         = data.aws_caller_identity.current.account_id

  tags = {
    Name = "${terraform.workspace}-eu-us"
  }

  provider = aws.eu-central-1
}

module "eu-us-peering-config" {
  source = "./peering"

  transit_gateway_peering_attachment_id = aws_ec2_transit_gateway_peering_attachment.eu-us-peering.id
  transit_gateway_id                    = module.eu-central-1.transit_gateway_id
  transit_gateway_route_table_id        = module.eu-central-1.transit_gateway_route_table_id
  vpc_route_table_ids                   = module.eu-central-1.vpc_route_table_ids
  peer_cidr_block                       = module.us-east-1.cidr_block

  providers = {
    aws = aws.eu-central-1
  }
}

module "us-eu-peering-config" {
  source = "./peering"

  transit_gateway_peering_attachment_id = aws_ec2_transit_gateway_peering_attachment.eu-us-peering.id
  transit_gateway_id                    = module.us-east-1.transit_gateway_id
  transit_gateway_route_table_id        = module.us-east-1.transit_gateway_route_table_id
  vpc_route_table_ids                   = module.us-east-1.vpc_route_table_ids
  peer_cidr_block                       = module.eu-central-1.cidr_block

  providers = {
    aws = aws.us-east-1
  }
}

# EU -> Singapore
resource "aws_ec2_transit_gateway_peering_attachment" "eu-ap-peering" {
  peer_region             = "ap-southeast-1"
  peer_transit_gateway_id = module.ap-southeast-1.transit_gateway_id
  transit_gateway_id      = module.eu-central-1.transit_gateway_id
  peer_account_id         = data.aws_caller_identity.current.account_id

  tags = {
    Name = "${terraform.workspace}-eu-ap"
  }

  provider = aws.eu-central-1
}

module "eu-ap-peering-config" {
  source = "./peering"

  transit_gateway_peering_attachment_id = aws_ec2_transit_gateway_peering_attachment.eu-ap-peering.id
  transit_gateway_id                    = module.eu-central-1.transit_gateway_id
  transit_gateway_route_table_id        = module.eu-central-1.transit_gateway_route_table_id
  vpc_route_table_ids                   = module.eu-central-1.vpc_route_table_ids
  peer_cidr_block                       = module.ap-southeast-1.cidr_block

  providers = {
    aws = aws.eu-central-1
  }
}

module "ap-eu-peering-config" {
  source = "./peering"

  transit_gateway_peering_attachment_id = aws_ec2_transit_gateway_peering_attachment.eu-ap-peering.id
  transit_gateway_id                    = module.ap-southeast-1.transit_gateway_id
  transit_gateway_route_table_id        = module.ap-southeast-1.transit_gateway_route_table_id
  vpc_route_table_ids                   = module.ap-southeast-1.vpc_route_table_ids
  peer_cidr_block                       = module.eu-central-1.cidr_block

  providers = {
    aws = aws.ap-southeast-1
  }
}

# US -> Singapore
resource "aws_ec2_transit_gateway_peering_attachment" "us-ap-peering" {
  peer_region             = "ap-southeast-1"
  peer_transit_gateway_id = module.ap-southeast-1.transit_gateway_id
  transit_gateway_id      = module.us-east-1.transit_gateway_id
  peer_account_id         = data.aws_caller_identity.current.account_id

  tags = {
    Name = "${terraform.workspace}-us-ap"
  }

  provider = aws.us-east-1
}

module "us-ap-peering-config" {
  source = "./peering"

  transit_gateway_peering_attachment_id = aws_ec2_transit_gateway_peering_attachment.us-ap-peering.id
  transit_gateway_id                    = module.us-east-1.transit_gateway_id
  transit_gateway_route_table_id        = module.us-east-1.transit_gateway_route_table_id
  vpc_route_table_ids                   = module.us-east-1.vpc_route_table_ids
  peer_cidr_block                       = module.ap-southeast-1.cidr_block

  providers = {
    aws = aws.us-east-1
  }
}

module "ap-us-peering-config" {
  source = "./peering"

  transit_gateway_peering_attachment_id = aws_ec2_transit_gateway_peering_attachment.us-ap-peering.id
  transit_gateway_id                    = module.ap-southeast-1.transit_gateway_id
  transit_gateway_route_table_id        = module.ap-southeast-1.transit_gateway_route_table_id
  vpc_route_table_ids                   = module.ap-southeast-1.vpc_route_table_ids
  peer_cidr_block                       = module.us-east-1.cidr_block

  providers = {
    aws = aws.ap-southeast-1
  }
}

module "dns_ap_extra" {
  count  = length(local.ap_southeast_1_extra_domains)
  source = "github.com/WalletConnect/terraform-modules/modules/dns"

  hosted_zone_name = local.ap_southeast_1_extra_domains[count.index].hosted_zone_name
  fqdn             = local.ap_southeast_1_extra_domains[count.index].fqdn

  providers = {
    aws = aws.ap-southeast-1
  }
}

# Extra DNS endpoints for the AP region
resource "aws_lb_listener_certificate" "relay_record_cert_ap_extra" {
  count           = length(module.ap-southeast-1.lb_listener_arns) * length(local.ap_southeast_1_extra_domains)
  listener_arn    = module.ap-southeast-1.lb_listener_arns[length(local.ap_southeast_1_extra_domains) > 1 ? count.index % length(module.ap-southeast-1.lb_listener_arns) : count.index]
  certificate_arn = module.dns_ap_extra[length(local.ap_southeast_1_extra_domains) > 1 ? length(local.ap_southeast_1_extra_domains) : 0].certificate_arn

  provider = aws.ap-southeast-1
}

resource "aws_route53_record" "relay_record_ap_extra" {
  count = length(module.ap-southeast-1.lb_listener_arns) * length(local.ap_southeast_1_extra_domains)
  depends_on = [
    aws_route53_record.relay_record_eu
  ]

  zone_id = module.dns_ap_extra[length(local.ap_southeast_1_extra_domains) > 1 ? length(local.ap_southeast_1_extra_domains) : 0].zone_id
  name    = local.ap_southeast_1_extra_domains[length(local.ap_southeast_1_extra_domains) > 1 ? length(local.ap_southeast_1_extra_domains) : 0].fqdn
  type    = "A"

  health_check_id = module.ap-southeast-1.health_check_id

  alias {
    name                   = module.ap-southeast-1.lb_dns_names[length(local.ap_southeast_1_extra_domains) > 1 ? count.index % length(module.ap-southeast-1.lb_listener_arns) : count.index]
    zone_id                = module.ap-southeast-1.lb_zone_ids[length(local.ap_southeast_1_extra_domains) > 1 ? count.index % length(module.ap-southeast-1.lb_listener_arns) : count.index]
    evaluate_target_health = true
  }

  weighted_routing_policy {
    weight = 100
  }

  set_identifier = "ap-lb-${length(local.ap_southeast_1_extra_domains) > 1 ? count.index % length(module.ap-southeast-1.lb_listener_arns) : count.index}"
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

data "aws_iam_policy_document" "ssm" {
  statement {
    actions = [
      "ssmmessages:CreateControlChannel",
      "ssmmessages:CreateDataChannel",
      "ssmmessages:OpenControlChannel",
      "ssmmessages:OpenDataChannel"
    ]
    resources = ["*"]
  }
}

resource "aws_iam_role" "irn_node_ecs_task_execution_role" {
  name               = "${local.environment}_irn_node_ecs_task_execution_role"
  assume_role_policy = data.aws_iam_policy_document.assume_role.json

  inline_policy {
    name   = "ssm-policy"
    policy = data.aws_iam_policy_document.ssm.json
  }
}

resource "aws_iam_role_policy_attachment" "irn_node_ecs_task_execution_role_policy" {
  role       = aws_iam_role.irn_node_ecs_task_execution_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
}

resource "aws_iam_role_policy_attachment" "irn_node_cloudwatch_write_policy" {
  role       = aws_iam_role.irn_node_ecs_task_execution_role.name
  policy_arn = "arn:aws:iam::aws:policy/CloudWatchLogsFullAccess"
}

resource "aws_iam_role_policy_attachment" "irn_node_prometheus_write_policy" {
  role       = aws_iam_role.irn_node_ecs_task_execution_role.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonPrometheusRemoteWriteAccess"
}

data "aws_iam_policy_document" "activemq_log_publishing_policy" {
  statement {
    actions = [
      "logs:CreateLogGroup",
      "logs:CreateLogStream",
      "logs:PutLogEvents",
      "logs:PutLogEventsBatch",
    ]

    resources = ["arn:aws:logs:*:*:log-group:/aws/amazonmq/*"]

    principals {
      identifiers = ["mq.amazonaws.com"]
      type        = "Service"
    }
  }
}

resource "aws_cloudwatch_log_resource_policy" "activemq_log_publishing_policy" {
  policy_document = data.aws_iam_policy_document.activemq_log_publishing_policy.json
  policy_name     = "activemq_log_publishing_policy"
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

resource "aws_iam_role" "irn_node_ec2_instance_role" {
  name               = "${local.environment}_irn_node_ec2_instance_role"
  assume_role_policy = data.aws_iam_policy_document.ec2_instance_policy.json
}

resource "aws_iam_role_policy_attachment" "irn_node_ec2_instance_role_attachment" {
  role       = aws_iam_role.irn_node_ec2_instance_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonEC2ContainerServiceforEC2Role"
}

resource "aws_iam_role_policy_attachment" "irn_node_ec2_instance_connect_policy" {
  role       = aws_iam_role.irn_node_ec2_instance_role.name
  policy_arn = "arn:aws:iam::aws:policy/EC2InstanceConnect"
}

resource "aws_iam_instance_profile" "irn_node_ec2_instance_profile" {
  name = "${local.environment}_irn_node_ec2_instance_profile"
  role = aws_iam_role.irn_node_ec2_instance_role.name
}

module "irn_node_keypair" {
  source = "./irn_node_keypair"
  for_each = toset([
    "eu-central-1a-1",
    "eu-central-1a-2",
    "eu-central-1b-1",
    "eu-central-1c-1",
    "us-east-1a-1",
    "us-east-1b-1",
    "us-east-1c-1",
    "ap-southeast-1a-1",
    "ap-southeast-1a-2",
    "ap-southeast-1b-1",
    "ap-southeast-1b-2",
    "ap-southeast-1c-1",
  ])
  node_id     = each.key
  environment = local.environment
}
