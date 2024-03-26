locals {
  pinned_latest_tag = sort(setsubtract(data.aws_ecr_image.service_image.image_tags, ["latest"]))[0]
  image_tag         = var.ecr_app_version == "latest" ? local.pinned_latest_tag : var.ecr_app_version
  image             = "${var.ecr_repository_url}:${local.image_tag}"

  # See: https://github.com/WalletConnect/aws-otel-collector
  otel_collector_image_tag = "v0.3.0"
  otel_collector_image     = "${var.aws_otel_collector_ecr_repository_url}:${local.otel_collector_image_tag}"

  file_descriptor_soft_limit = pow(2, 18)
  file_descriptor_hard_limit = local.file_descriptor_soft_limit * 2
}

# Log Group for our App
resource "aws_cloudwatch_log_group" "cluster_logs" {
  name              = "${var.app_name}_logs"
  retention_in_days = 14
}

data "aws_ecr_image" "service_image" {
  repository_name = "relay"
  image_tag       = var.ecr_app_version
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
}

# ECS Task definition
resource "aws_ecs_task_definition" "app_task" {
  family                   = var.app_name
  container_definitions    = <<DEFINITION
  [
    {
      "name": "${var.app_name}",
      "environment" : [
          { "name" : "RELAY_STORAGE_WEBHOOK_AMQP_ADDR_PRIMARY", "value" : "${var.webhook_amqp_addr_primary}" },
          { "name" : "RELAY_STORAGE_WEBHOOK_AMQP_ADDR_SECONDARY", "value" : "${var.webhook_amqp_addr_secondary}" },
          { "name" : "RELAY_STORAGE_WATCH_AMQP_ADDR_PRIMARY", "value" : "${var.webhook_amqp_addr_primary}" },
          { "name" : "RELAY_STORAGE_WATCH_AMQP_ADDR_SECONDARY", "value" : "${var.webhook_amqp_addr_secondary}" },
          { "name" : "RELAY_STORAGE_REPLICATION_LAG_MS", "value" : "${var.storage_replication_lag_ms}" },
          { "name" : "RELAY_STORAGE_IRN_NAMESPACE_SECRET", "value" : "${var.irn_namespace_secret}" },
          { "name" : "RELAY_RPC_RATE_LIMIT", "value" : "60" },
          { "name" : "RELAY_RPC_RATE_LIMIT_INTERVAL_MS", "value" : "60000" },
          { "name" : "RELAY_RPC_GLOBAL_RATE_LIMIT_MULTIPLIER", "value" : "100" },
          { "name" : "RELAY_RPC_RATE_LIMIT_WHITELIST", "value" : "${var.rpc_rate_limit_whitelist}" },
          { "name" : "LOG_LEVEL", "value" : "${var.log_level}" },
          { "name" : "LOG_LEVEL_OTEL", "value" : "${var.telemetry_level}" },
          { "name" : "RELAY_ANALYTICS_EXPORT_BUCKET", "value" : "${var.analytics-data-lake_bucket_name}" },
          { "name" : "RELAY_ANALYTICS_GEOIP_DB_BUCKET", "value" : "${var.analytics_geoip_db_bucket_name}" },
          { "name" : "RELAY_ANALYTICS_GEOIP_DB_KEY", "value" : "${var.analytics_geoip_db_key}" },
          { "name" : "RELAY_REGISTRY_PROJECT_DATA_CACHE_TTL", "value" : "${var.project_data_cache_ttl}" },
          { "name" : "RELAY_REGISTRY_API_URL", "value" : "${var.registry_api_endpoint}" },
          { "name" : "RELAY_REGISTRY_API_AUTH_TOKEN", "value" : "${var.registry_api_auth_token}" },
          { "name" : "RELAY_ED25519_PRIVATE_KEY", "value" : "${var.ed25519_private_key}" },
          { "name" : "RELAY_WEBSOCKET_SESSION_LENGTH_MS", "value" : "${var.disconnect_timeout_ms}" },
          { "name" : "RELAY_WEBSOCKET_SESSION_LENGTH_JITTER_MS", "value" : "${var.disconnect_timeout_ms}" },
          { "name" : "RELAY_WEBSOCKET_HEARTBEAT_INTERVAL_MS", "value" : "${var.heartbeat_interval_ms}" },
          { "name" : "RELAY_WEBSOCKET_HEARTBEAT_TIMEOUT_MS", "value" : "${var.heartbeat_timeout_ms}" },
          { "name" : "RELAY_AUTH_AUD", "value" : "wss://${join(",wss://", var.auth_fqdns)},https://${join(",https://", var.auth_fqdns)}" },
          { "name" : "RELAY_DOMAIN", "value" : "${var.environment}" },
          { "name" : "RELAY_REGION", "value" : "${var.region}" },
          { "name" : "OTEL_SERVICE_NAME", "value" : "${var.app_name}" },
          { "name" : "OTEL_RESOURCE_ATTRIBUTES", "value" : "environment=${var.environment},region=${var.region}" },
          { "name" : "OTEL_EXPORTER_OTLP_ENDPOINT", "value" : "http://localhost:4317" },
          { "name" : "OTEL_TRACES_SAMPLER", "value" : "traceidratio" },
          { "name" : "OTEL_TRACES_SAMPLER_ARG", "value" : "${var.telemetry_sample_ratio}" },
          { "name" : "RELAY_IRN_STAGING_SHADOWING_FACTOR", "value" : "0.1" },
          { "name" : "RELAY_IRN_NODES", "value" : "${join(",", [for peer_id, ip in var.irn_nodes : "${peer_id}-/ip4/${ip}/udp/${var.irn_api_port}/quic-v1"])}" },
          { "name" : "RELAY_IRN_SHADOWING_NODES", "value" : "${join(",", [for peer_id, ip in var.irn_shadowing_nodes : "${peer_id}-/ip4/${ip}/udp/${var.irn_api_port}/quic-v1"])}" },
          { "name" : "RELAY_BLOCKED_COUNTRIES", "value" : "${var.ofac_countries}" },
          { "name" : "CACHE_BUSTER", "value" : "${var.cache_buster}" }
      ],
      "image": "${local.image}",
      "essential": true,
      "portMappings": [
        {
          "containerPort": ${var.public_port},
          "hostPort": ${var.public_port}
        },
        {
          "containerPort": ${var.private_port},
          "hostPort": ${var.private_port}
        }
      ],
      "memory": ${var.relay_node_memory},
      "cpu": ${var.relay_node_cpu},
      "ulimits": [{
        "name": "nofile",
        "softLimit": ${local.file_descriptor_soft_limit},
        "hardLimit": ${local.file_descriptor_hard_limit}
      }],
      "logConfiguration": {
        "logDriver": "awslogs",
        "options": {
          "awslogs-group": "${aws_cloudwatch_log_group.cluster_logs.name}",
          "awslogs-region": "${var.region}",
          "awslogs-stream-prefix": "ecs"
        }
      },
      "dependsOn": [{
        "containerName": "aws-otel-collector",
        "condition": "START"
      }]
    },
    {
      "name": "aws-otel-collector",
      "image": "${local.otel_collector_image}",
      "environment" : [
          { "name" : "AWS_PROMETHEUS_SCRAPING_ENDPOINT", "value" : "0.0.0.0:${var.private_port}" },
          { "name" : "AWS_PROMETHEUS_ENDPOINT", "value" : "${var.prometheus_endpoint}api/v1/remote_write" },
          { "name" : "AWS_REGION", "value" : "eu-central-1" }
      ],
      "portMappings": [
        {
          "hostPort": 4317,
          "protocol": "tcp",
          "containerPort": 4317
        }
      ],
      "essential": true,
      "command": [
        "--config=/walletconnect/relay.yaml"
      ],
      "logConfiguration": {
        "logDriver": "awslogs",
        "options": {
          "awslogs-create-group": "True",
          "awslogs-group": "/ecs/${var.app_name}-ecs-aws-otel-sidecar-collector",
          "awslogs-region": "${var.region}",
          "awslogs-stream-prefix": "ecs"
        }
      }
    }
  ]
  DEFINITION
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  memory                   = var.relay_node_memory
  cpu                      = var.relay_node_cpu
  execution_role_arn       = aws_iam_role.ecs_task_execution_role.arn
  task_role_arn            = aws_iam_role.ecs_task_execution_role.arn

  runtime_platform {
    operating_system_family = "LINUX"
  }
}

resource "aws_iam_role" "ecs_task_execution_role" {
  name               = "${var.app_name}_ecs_task_execution_role"
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

resource "aws_iam_role_policy_attachment" "prometheus_write_policy" {
  role       = aws_iam_role.ecs_task_execution_role.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonPrometheusRemoteWriteAccess"
}

resource "aws_iam_role_policy_attachment" "cloudwatch_write_policy" {
  role       = aws_iam_role.ecs_task_execution_role.name
  policy_arn = "arn:aws:iam::aws:policy/CloudWatchLogsFullAccess"
}

resource "aws_iam_policy" "xray_telemetry_access" {
  name        = "${var.app_name}_xray_telemetry_access"
  path        = "/"
  description = "Allows ${var.app_name} to create/write log streams and x-ray telemetry"

  policy = jsonencode({
    "Version" : "2012-10-17",
    "Statement" : [
      {
        "Effect" : "Allow",
        "Action" : [
          "logs:PutLogEvents",
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:DescribeLogStreams",
          "logs:DescribeLogGroups",
          "xray:PutTraceSegments",
          "xray:PutTelemetryRecords",
          "xray:GetSamplingRules",
          "xray:GetSamplingTargets",
          "xray:GetSamplingStatisticSummaries",
          "ssm:GetParameters"
        ],
        "Resource" : "*"
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "xray-telemetry-attach" {
  role       = aws_iam_role.ecs_task_execution_role.name
  policy_arn = aws_iam_policy.xray_telemetry_access.arn
}

# ECS Service
resource "aws_ecs_service" "app_service" {
  name            = "${var.app_name}-service-${substr(uuid(), 0, 3)}"
  cluster         = aws_ecs_cluster.app_cluster.id
  task_definition = join(":", slice(split(":", aws_ecs_task_definition.app_task.arn), 0, 6))
  launch_type     = "FARGATE"
  desired_count   = var.autoscaling_min_capacity
  propagate_tags  = "TASK_DEFINITION"

  # Wait for the service deployment to succeed
  wait_for_steady_state = true

  network_configuration {
    subnets          = var.private_subnet_ids
    assign_public_ip = false                                                                                # We do public ingress through the LB
    security_groups  = ["${aws_security_group.tls_ingress.id}", "${aws_security_group.vpc_app_ingress.id}"] # Setting the security group
  }

  dynamic "load_balancer" {
    for_each = aws_lb_target_group.target_group
    content {
      target_group_arn = load_balancer.value.arn
      container_name   = aws_ecs_task_definition.app_task.family
      container_port   = var.public_port
    }
  }

  # Allow external changes without Terraform plan difference
  lifecycle {
    create_before_destroy = true
    ignore_changes        = [desired_count, name]
  }
}

# Load Balancer
resource "aws_alb" "network_load_balancer" {
  count = var.lb_count

  name               = replace(replace(replace("${var.app_name}-lb-${substr(uuid(), 0, 3)}", "_", "-"), "southeast", "se"), "staging", "sta")
  load_balancer_type = "network"
  subnets            = var.public_subnet_ids

  lifecycle {
    create_before_destroy = true
    ignore_changes        = [name]
  }
}

resource "aws_lb_target_group" "target_group" {
  count = var.lb_count

  name        = replace("${var.app_name}-${substr(uuid(), 0, 3)}", "_", "-")
  port        = 80
  protocol    = "TCP"
  target_type = "ip"
  vpc_id      = var.vpc_id # Referencing the default VPC

  stickiness {
    enabled = false
    type    = "source_ip"
  }

  preserve_client_ip = true

  deregistration_delay = var.lb_deregistration_delay_secs

  health_check {
    protocol            = "HTTP"
    path                = "/health"
    interval            = 10
    healthy_threshold   = 5
    unhealthy_threshold = 5
  }

  lifecycle {
    create_before_destroy = true
    ignore_changes        = [name]
  }
}

resource "aws_lb_listener" "listener" {
  count = var.lb_count

  load_balancer_arn = aws_alb.network_load_balancer[count.index].arn
  port              = "443"
  protocol          = "TLS"
  certificate_arn   = var.acm_certificate_arn
  ssl_policy        = "ELBSecurityPolicy-TLS13-1-2-2021-06"
  default_action {
    type             = "forward"
    target_group_arn = aws_lb_target_group.target_group[count.index].arn
  }

  lifecycle {
    create_before_destroy = true
  }
}

# Security Groups
resource "aws_security_group" "tls_ingress" {
  name        = "${var.app_name}-tls-to-lb"
  description = "Allow tls ingress from everywhere"
  vpc_id      = var.vpc_id
  ingress {
    from_port   = 443 # Allowing traffic in from port 443
    to_port     = 443
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"] # Allowing traffic in from all sources
  }

  egress {
    from_port   = 0             # Allowing any incoming port
    to_port     = 0             # Allowing any outgoing port
    protocol    = "-1"          # Allowing any outgoing protocol
    cidr_blocks = ["0.0.0.0/0"] # Allowing traffic out to all IP addresses
  }

  tags = {
    Name = "${var.app_name}-vpc-tls-ingress"
  }
}

resource "aws_security_group" "vpc_app_ingress" {
  name        = "${var.app_name}-vpc-to-app"
  description = "Allow app port ingress from vpc"
  vpc_id      = var.vpc_id

  ingress {
    from_port   = var.public_port
    to_port     = var.public_port
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  ingress {
    from_port   = var.private_port
    to_port     = var.private_port
    protocol    = "tcp"
    cidr_blocks = var.vpc_cidr_blocks
  }

  egress {
    from_port   = 0             # Allowing any incoming port
    to_port     = 0             # Allowing any outgoing port
    protocol    = "-1"          # Allowing any outgoing protocol
    cidr_blocks = ["0.0.0.0/0"] # Allowing traffic out to all IP addresses
  }

  tags = {
    Name = "${var.app_name}-app-tcp-ingress"
  }
}

# DNS Records
resource "aws_route53_record" "dns_load_balancer" {
  count = var.lb_count

  zone_id = var.route53_zone_id
  name    = "lb-${count.index}.${var.fqdn}"
  type    = "A"

  alias {
    name                   = aws_alb.network_load_balancer[count.index].dns_name
    zone_id                = aws_alb.network_load_balancer[count.index].zone_id
    evaluate_target_health = true
  }
}

resource "aws_route53_record" "dnsset" {
  depends_on = [
    aws_route53_record.dns_load_balancer
  ]
  count = var.lb_count

  zone_id = var.route53_zone_id
  name    = var.fqdn
  type    = "A"

  alias {
    name                   = aws_alb.network_load_balancer[count.index].dns_name
    zone_id                = aws_alb.network_load_balancer[count.index].zone_id
    evaluate_target_health = true
  }

  weighted_routing_policy {
    weight = 100
  }

  set_identifier = "lb-${count.index}"
}

# Autoscaling
# We can scale by
# ECSServiceAverageCPUUtilization, ECSServiceAverageMemoryUtilization, and ALBRequestCountPerTarget
# out of the box or use custom metrics
resource "aws_appautoscaling_target" "ecs_target" {
  max_capacity       = var.autoscaling_max_capacity
  min_capacity       = var.autoscaling_min_capacity
  resource_id        = "service/${aws_ecs_cluster.app_cluster.name}/${aws_ecs_service.app_service.name}"
  scalable_dimension = "ecs:service:DesiredCount"
  service_namespace  = "ecs"
}

resource "aws_appautoscaling_policy" "cpu_scaling" {
  name               = "${var.app_name}-application-scaling-policy-cpu"
  policy_type        = "TargetTrackingScaling"
  resource_id        = aws_appautoscaling_target.ecs_target.resource_id
  scalable_dimension = aws_appautoscaling_target.ecs_target.scalable_dimension
  service_namespace  = aws_appautoscaling_target.ecs_target.service_namespace

  target_tracking_scaling_policy_configuration {
    predefined_metric_specification {
      predefined_metric_type = "ECSServiceAverageCPUUtilization"
    }
    target_value       = 30
    scale_in_cooldown  = 180
    scale_out_cooldown = 90
  }
  depends_on = [aws_appautoscaling_target.ecs_target]
}

resource "aws_appautoscaling_policy" "memory_scaling" {
  name               = "${var.app_name}-application-scaling-policy-memory"
  policy_type        = "TargetTrackingScaling"
  resource_id        = aws_appautoscaling_target.ecs_target.resource_id
  scalable_dimension = aws_appautoscaling_target.ecs_target.scalable_dimension
  service_namespace  = aws_appautoscaling_target.ecs_target.service_namespace

  target_tracking_scaling_policy_configuration {
    predefined_metric_specification {
      predefined_metric_type = "ECSServiceAverageMemoryUtilization"
    }
    target_value       = 30
    scale_in_cooldown  = 180
    scale_out_cooldown = 90
  }
  depends_on = [aws_appautoscaling_target.ecs_target]
}
