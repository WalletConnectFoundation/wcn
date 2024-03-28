locals {
  app_name = "relay"

  environment = terraform.workspace
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

data "aws_ecr_repository" "aws_otel_collector" {
  name = "aws-otel-collector"
}

data "aws_prometheus_workspaces" "this" {
  alias_prefix = "prometheus-${terraform.workspace}-${local.app_name}"
}

data "aws_prometheus_workspace" "this" {
  workspace_id = data.aws_prometheus_workspaces.this.workspace_ids[0]
}

module "monitoring" {
  source = "./monitoring"

  environment             = terraform.workspace
  prometheus_workspace_id = data.aws_prometheus_workspace.this.workspace_id
  irn_nodes               = [for _, node in merge(module.eu-central-1.irn_nodes, module.us-east-1.irn_nodes, module.ap-southeast-1.irn_nodes) : node]

  providers = {
    aws = aws.eu-central-1
  }
}

module "eu-central-1" {
  source                                = "./region"
  environment                           = terraform.workspace
  region                                = "eu-central-1"
  aws_otel_collector_ecr_repository_url = data.aws_ecr_repository.aws_otel_collector.repository_url
  irn_node_cpu                          = var.irn_node_cpu
  irn_node_memory                       = var.irn_node_memory
  irn_node_log_level                    = var.irn_node_log_level
  irn_cache_buster                      = var.irn_cache_buster

  prometheus_endpoint = data.aws_prometheus_workspace.this.prometheus_endpoint
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
  aws_otel_collector_ecr_repository_url = replace(data.aws_ecr_repository.aws_otel_collector.repository_url, "eu-central-1", "us-east-1")
  irn_node_cpu                          = var.irn_node_cpu
  irn_node_memory                       = var.irn_node_memory
  irn_node_log_level                    = var.irn_node_log_level
  irn_cache_buster                      = var.irn_cache_buster

  prometheus_endpoint = data.aws_prometheus_workspace.this.prometheus_endpoint
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
  aws_otel_collector_ecr_repository_url = replace(data.aws_ecr_repository.aws_otel_collector.repository_url, "eu-central-1", "ap-southeast-1")
  irn_node_cpu                          = var.irn_node_cpu
  irn_node_memory                       = var.irn_node_memory
  irn_node_log_level                    = var.irn_node_log_level
  irn_cache_buster                      = var.irn_cache_buster

  prometheus_endpoint = data.aws_prometheus_workspace.this.prometheus_endpoint

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
