locals {
  cpu    = var.cpu
  memory = local.cpu * 2
}

# Log Group for our Canary
resource "aws_cloudwatch_log_group" "cluster_logs" {
  name              = "${var.app_name}_logs"
  retention_in_days = 14
}

# ECS Cluster
resource "aws_ecs_cluster" "app_cluster" {
  name = "${var.app_name}_cluster"

  configuration {
    execute_command_configuration {
      logging = "OVERRIDE"

      log_configuration {
        cloud_watch_encryption_enabled = false
        cloud_watch_log_group_name     = aws_cloudwatch_log_group.cluster_logs.name
      }
    }
  }

  # Exposes metrics such as the
  # number of running tasks
  # in CloudWatch
  setting {
    name  = "containerInsights"
    value = "enabled"
  }

  tags = {
    ChildApp = "Canary"
  }
}

# ECS Task definition
resource "aws_ecs_task_definition" "app_task" {
  family                   = var.app_name
  container_definitions    = <<DEFINITION
  [
    {
      "name": "${var.app_name}",
      "command": ${var.command},
      "environment" : [
          { "name" : "TEST_RELAY_URL", "value" : "wss://${var.target_endpoint}" },
          { "name" : "TEST_PROJECT_ID", "value" : "${var.canary_project_id}" },
          { "name" : "NEXT_PUBLIC_PROJECT_ID", "value" : "${var.canary_project_id}" },
          { "name" : "ENVIRONMENT", "value" : "${var.environment}" },
          { "name" : "STATUSPAGE_API_KEY", "value" : "${var.statuspage_api_key}" },
          { "name" : "REGION", "value" : "${var.region}" },
          { "name" : "TAG", "value" : "${var.tag}" },
          { "name" : "CI", "value" : "true" },
          { "name" : "SKIP_PLAYWRIGHT_WEBSERVER", "value" : "true" },
          { "name" : "BASE_URL", "value" : "https://lab.web3modal.com/" }
      ],
      "image": "${var.ecr_repository_url}:${var.ecr_image_tag}",
      "essential": true,
      "memory": ${local.memory},
      "cpu": ${local.cpu},
      "logConfiguration": {
        "logDriver": "awslogs",
        "options": {
          "awslogs-group": "${aws_cloudwatch_log_group.cluster_logs.name}",
          "awslogs-region": "${var.region}",
          "awslogs-stream-prefix": "ecs"
        }
      }
    }
  ]
  DEFINITION
  requires_compatibilities = ["FARGATE"]  # Stating that we are using ECS Fargate
  network_mode             = "awsvpc"     # Using awsvpc as our network mode as this is required for Fargate
  memory                   = local.memory # Specifying the memory our container requires
  cpu                      = local.cpu    # Specifying the CPU our container requires
  execution_role_arn       = aws_iam_role.ecs_task_execution_role.arn
  task_role_arn            = aws_iam_role.ecs_task_execution_role.arn

  runtime_platform {
    operating_system_family = "LINUX"
  }
}

resource "aws_iam_role" "ecs_task_execution_role" {
  name               = "${var.app_name}_ecs_role"
  assume_role_policy = data.aws_iam_policy_document.assume_role_policy.json
}

data "aws_iam_policy_document" "assume_role_policy" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["ecs-tasks.amazonaws.com"]
    }
  }
}

resource "aws_iam_role_policy_attachment" "ecs_task_execution_role_policy" {
  role       = aws_iam_role.ecs_task_execution_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
}

resource "aws_iam_role_policy_attachment" "cloudwatch_policy" {
  role       = aws_iam_role.ecs_task_execution_role.name
  policy_arn = "arn:aws:iam::aws:policy/CloudWatchFullAccess"
}

resource "aws_security_group" "vpc_app_egress" {
  name        = "${var.app_name}-vpc-egress-from-canary"
  description = "Allow app port egress from vpc"
  vpc_id      = var.vpc_id

  egress {
    from_port   = 0             # Allowing any incoming port
    to_port     = 0             # Allowing any outgoing port
    protocol    = "-1"          # Allowing any outgoing protocol 
    cidr_blocks = ["0.0.0.0/0"] # Allowing traffic out to all IP addresses
  }

  tags = {
    Name = "${var.app_name}-vpc-egress-from-canary"
  }
}

module "eventbridge" {
  source  = "terraform-aws-modules/eventbridge/aws"
  version = "2.3.0"

  # Schedules can only be created on default bus
  create_bus = false

  create_role       = true
  role_name         = "${var.app_name}-eventbridge"
  attach_ecs_policy = true
  ecs_target_arns   = [aws_ecs_task_definition.app_task.arn]

  rules = {
    "${var.app_name}" = {
      description         = "Cron for ${var.app_name}"
      enabled             = true
      schedule_expression = var.schedule
    }
  }

  # Send to a fargate ECS cluster
  targets = {
    "${var.app_name}" = [
      {
        name            = "${var.app_name}"
        arn             = aws_ecs_cluster.app_cluster.arn
        attach_role_arn = true

        ecs_target = {
          launch_type         = "FARGATE"
          task_count          = 1
          task_definition_arn = aws_ecs_task_definition.app_task.arn

          network_configuration = {
            assign_public_ip = false
            subnets          = var.private_subnet_ids
            security_groups  = [aws_security_group.vpc_app_egress.id]
          }

          tags = {
            Application = "relay"
            Name        = "relay-canary"
          }
        }
      }
    ]
  }
}
