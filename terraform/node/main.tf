locals {
  name = "${var.environment}-${var.id}-irn-node"

  # filter out the peer_id of the current node from the map
  known_peers = { for k, v in var.known_peers : k => v if k != var.peer_id }

  # Amazon Linux AMI 2023.0.20231204 x86_64 ECS HVM EBS
  # The same image has different ids per region.
  # Changing this would require replacement of the instances, so do this very carefully.
  # It's hard-coded, because querying it in runtime is error prone and produces dirty terraform plans
  # in which we not sure whether the instance is being replaced or not.
  ami_id = {
    "eu-central-1"   = "ami-0413ba1f2f7b7a478"
    "us-east-1"      = "ami-09b21680970b4eb8b"
    "ap-southeast-1" = "ami-0b2249b9e9a5fe80e"
  }

  tags = merge(var.tags, { Name = local.name })
}

resource "aws_subnet" "this" {
  vpc_id = var.vpc_id

  # `eu-central-1a-1` -> `eu-central-1a`
  availability_zone = join("-", slice(split("-", var.id), 0, 3))

  # `10.1.111.10` -> `10.1.111.0/24`
  cidr_block = format("%s.0/24", join(".", slice(split(".", var.ipv4_address), 0, 3)))

  tags = local.tags
}

# Connect the subnet to a NAT gateway defined outside of this module.
resource "aws_route_table_association" "this" {
  route_table_id = var.route_table_id
  subnet_id      = aws_subnet.this.id
}

resource "aws_cloudwatch_log_group" "this" {
  name              = "${local.name}-logs"
  retention_in_days = 14

  tags = local.tags
}

resource "aws_ebs_volume" "this" {
  availability_zone = aws_subnet.this.availability_zone
  type              = "gp3"
  size              = var.ebs_volume_size

  tags = local.tags
}

resource "aws_instance" "this" {
  ami           = local.ami_id[var.region]
  instance_type = var.ec2_instance_type

  user_data = templatefile("${path.module}/ec2_instance_user_data.sh", { ecs_cluster = aws_ecs_cluster.this.name })

  iam_instance_profile = var.ec2_instance_profile

  subnet_id  = aws_subnet.this.id
  private_ip = var.ipv4_address

  vpc_security_group_ids = var.security_group_ids

  depends_on = [aws_ebs_volume.this]

  tags = local.tags
}

resource "aws_eip_association" "this" {
  count = var.expose_public_ip ? 1 : 0

  instance_id   = aws_instance.this.id
  allocation_id = var.eip_id
}

data "aws_eip" "this" {
  count = var.expose_public_ip ? 1 : 0

  id = var.eip_id
}

resource "aws_volume_attachment" "this" {
  device_name = "/dev/xvdb"
  volume_id   = aws_ebs_volume.this.id
  instance_id = aws_instance.this.id
}

resource "aws_ecs_cluster" "this" {
  name = local.name

  configuration {
    execute_command_configuration {
      logging = "OVERRIDE"

      log_configuration {
        cloud_watch_encryption_enabled = false
        cloud_watch_log_group_name     = aws_cloudwatch_log_group.this.name
      }
    }
  }

  # Exposes metrics such as the number of running tasks in CloudWatch
  setting {
    name  = "containerInsights"
    value = "enabled"
  }

  tags = local.tags
}

resource "tls_private_key" "this" {
  algorithm   = "ECDSA"
  ecdsa_curve = "P384"
}

resource "tls_self_signed_cert" "this" {
  private_key_pem = tls_private_key.this.private_key_pem

  subject {
    organization = "WalletConnect"
  }

  ip_addresses = [local.addr]

  validity_period_hours = 24 * 365 # a year

  allowed_uses = [
    "key_encipherment",
    "digital_signature",
    "server_auth",
    "cert_signing",
  ]
}

locals {
  addr = var.expose_public_ip ? data.aws_eip.this[0].public_ip : var.ipv4_address

  irn_container_definition = {
    name = local.name
    environment = concat([
      { name = "ADDR", value = "/ip4/${local.addr}/udp/${var.libp2p_port}/quic-v1" },
      { name = "API_ADDR", value = "/ip4/${local.addr}/udp/${var.api_port}/quic-v1" },
      { name = "METRICS_ADDR", value = "0.0.0.0:${var.metrics_port}" },
      { name = "REPLICATION_STRATEGY_FACTOR", value = "3" },
      { name = "REPLICATION_STRATEGY_LEVEL", value = "Quorum" },
      { name = "LOG_LEVEL", value = "${var.log_level}" },
      { name = "SECRET_KEY", value = "${var.secret_key}" },
      { name = "GROUP", value = var.group_id },
      { name = "ROCKSDB_DIR", value = "/irn/rocksdb" },
      { name = "ROCKSDB_NUM_BATCH_THREADS", value = "8" },
      { name = "ROCKSDB_NUM_CALLBACK_THREADS", value = "32" },
      { name = "REQUEST_CONCURRENCY_LIMIT", value = "4500" },
      { name = "REQUEST_LIMITER_QUEUE", value = "65536" },
      { name = "NETWORK_CONNECTION_TIMEOUT", value = "1000" },
      { name = "NETWORK_REQUEST_TIMEOUT", value = "5000" },
      { name = "REPLICATION_REQUEST_TIMEOUT", value = "2000" },
      { name = "WARMUP_DELAY", value = "45000" },
      { name = "RAFT_DIR", value = "/irn/raft" },
      { name = "IS_RAFT_MEMBER", value = "false" }, # Bootstrap nodes are members regardless of this setting
      { name = "MIGRATION_TEST", value = var.id == "c" ? "true" : "false" },
      { name = "CACHE_BUSTER", value = var.cache_buster }
      ],
      length(var.bootstrap_nodes) != 0 ? [
        { name = "BOOTSTRAP_NODES", value = "${join(",", var.bootstrap_nodes)}" },
      ] : [],
      [for peer_id, ip in local.known_peers : {
        name  = "PEER_${peer_id}",
        value = "/ip4/${ip}/udp/${var.libp2p_port}/quic-v1"
      }],
      var.authorized_raft_candidates != null ? [
        { name = "AUTHORIZED_RAFT_CANDIDATES", value = "${join(",", var.authorized_raft_candidates)}" },
      ] : [],
      var.authorized_clients != null ? [
        { name = "AUTHORIZED_CLIENTS", value = "${join(",", var.authorized_clients)}" },
      ] : [],
      var.config_smart_contract_address != null ? [
        { name = "CONFIG_SMART_CONTRACT_ADDRESS", value = var.config_smart_contract_address },
      ] : [],
      var.smart_contract_signer_mnemonic != null ? [
        { name = "SMART_CONTRACT_SIGNER_MNEMONIC", value = var.smart_contract_signer_mnemonic },
        { name = "PERFORMANCE_TRACKER_DIR", value = "/irn/performance-tracker" },
      ] : [],
      var.eth_rpc_url != null ? [
        { name = "ETH_RPC_URL", value = var.eth_rpc_url },
      ] : [],
      var.eth_address != null ? [
        { name = "ETH_ADDRESS", value = var.eth_address },
      ] : [],
    )

    image     = var.image
    essential = true
    portMappings = [
      {
        hostPort      = var.libp2p_port
        containerPort = var.libp2p_port
        protocol      = "udp"
      },
      {
        hostPort      = var.api_port
        containerPort = var.api_port
        protocol      = "udp"
      },
      {
        hostPort      = var.metrics_port
        containerPort = var.metrics_port
        protocol      = "tcp"
      },
    ]
    memory = var.node_memory
    cpu    = var.node_cpu
    mountPoints = [{
      containerPath = "/irn"
      sourceVolume  = "data"
    }]
    logConfiguration = {
      logDriver = "awslogs"
      options = {
        awslogs-group         = "${aws_cloudwatch_log_group.this.name}"
        awslogs-region        = "${var.region}"
        awslogs-stream-prefix = "ecs"
      }
    }

    # The whole instance only runs to serve this one container, basically. So it's ok to give it 
    # all the permissions.
    #
    # For now we only need it to do profiling.
    privileged = true

    # We define this dependency to force other containers not to exit prematurely, when the node waits for
    # its turn for restart.
    # Without having this we lose 30-45s of metrics during re-deployments.
    dependsOn = concat(
      var.enable_otel_collector ? [{
        containerName = local.otel_container_name
        condition     = "START"
      }] : [],
      var.enable_prometheus ? [{
        containerName = local.prometheus_container_name
        condition     = "START"
      }] : [],
    )
  }

  otel_container_name = "aws-otel-collector"
  otel_container_definition = {
    name : local.otel_container_name,
    image : var.aws_otel_collector_image,
    environment : [
      { name : "AWS_PROMETHEUS_SCRAPING_ENDPOINT", value : "0.0.0.0:${var.metrics_port}" },
      { name : "AWS_PROMETHEUS_ENDPOINT", value : "${var.prometheus_endpoint}api/v1/remote_write" },
      { name : "AWS_REGION", value : "eu-central-1" },
      { name : "AOT_CONFIG_CONTENT", value : file("${path.module}/aws-otel-collector.yml") }
    ],
    portMappings : [
      {
        hostPort : 4317,
        protocol : "tcp",
        containerPort : 4317
      }
    ],
    essential : true,
    logConfiguration : {
      logDriver : "awslogs",
      options : {
        awslogs-create-group : "True",
        awslogs-group : "/ecs/${local.name}-ecs-aws-otel-sidecar-collector",
        awslogs-region : "${var.region}",
        awslogs-stream-prefix : "ecs"
      }
    }
  }

  prometheus_dir            = "/data/prometheus"
  prometheus_container_name = "prometheus"
  prometheus_config = {
    env    = "CONFIG"
    "path" = "${local.prometheus_dir}/config.yml"
    content = yamlencode({
      scrape_configs = [for peer_id in var.prometheus_peer_ids : {
        job_name        = "node-${peer_id}"
        metrics_path    = "/metrics/${peer_id}"
        scrape_interval = "15s"
        static_configs = [{
          targets = ["localhost:${var.metrics_port}"]
        }]
      }]
    })
  }
  prometheus_web_config = {
    env    = "WEB_CONFIG"
    "path" = "${local.prometheus_dir}/web.config.yml"
    content = yamlencode({
      tls_server_config = {
        cert_file = local.prometheus_tls_cert.path
        key_file  = local.prometheus_tls_key.path
      }
      basic_auth_users = {
        admin = bcrypt(var.prometheus_admin_password)
      }
    })
  }
  prometheus_tls_cert = {
    env     = "TLS_CERT"
    "path"  = "${local.prometheus_dir}/tls.cert"
    content = tls_self_signed_cert.this.cert_pem
  }
  prometheus_tls_key = {
    env     = "TLS_KEY"
    "path"  = "${local.prometheus_dir}/tls.key"
    content = tls_private_key.this.private_key_pem
  }
  prometheus_container_definition = {
    name : local.prometheus_container_name,
    image : var.prometheus_image,
    environment : [
      for e in [local.prometheus_config, local.prometheus_web_config, local.prometheus_tls_cert, local.prometheus_tls_key] :
      { name : e.env, value = e.content }
    ],

    user : "1001",
    entryPoint : ["/bin/sh"],
    command : [
      "-c",
      <<-CMD
        mkdir -p /data/prometheus && \
        printenv ${local.prometheus_config.env} > ${local.prometheus_config.path} && \
        printenv ${local.prometheus_web_config.env} > ${local.prometheus_web_config.path} && \
        printenv ${local.prometheus_tls_cert.env} > ${local.prometheus_tls_cert.path} && \
        printenv ${local.prometheus_tls_key.env} > ${local.prometheus_tls_key.path} && \
        exec /bin/prometheus \
        --config.file=${local.prometheus_config.path} \
        --web.config.file=${local.prometheus_web_config.path} \
        --storage.tsdb.path=/data/prometheus \
        --web.console.libraries=/usr/share/prometheus/console_libraries \
        --web.console.templates=/usr/share/prometheus/consoles
      CMD
    ]

    portMappings : [
      {
        hostPort : 9090,
        protocol : "tcp",
        containerPort : 9090
      }
    ],

    mountPoints = [{
      containerPath = "/data"
      sourceVolume  = "data"
    }]

    logConfiguration : {
      logDriver : "awslogs",
      options : {
        awslogs-create-group : "True",
        awslogs-group : "/ecs/${local.name}-prometheus",
        awslogs-region : "${var.region}",
        awslogs-stream-prefix : "ecs"
      }
    }
  }


  grafana_dir            = "/data/grafana"
  grafana_container_name = "grafana"
  grafana_tls_cert = {
    env     = "TLS_CERT"
    "path"  = "${local.grafana_dir}/tls.cert"
    content = tls_self_signed_cert.this.cert_pem
  }
  grafana_tls_key = {
    env     = "TLS_KEY"
    "path"  = "${local.grafana_dir}/tls.key"
    content = tls_private_key.this.private_key_pem
  }
  grafana_datasources = {
    env    = "DATASOURCES"
    "path" = "${local.grafana_dir}/provisioning/datasources/default.yaml"
    content = yamlencode({
      apiVersion = 1
      datasources = [{
        name          = "Prometheus"
        type          = "prometheus"
        access        = "proxy"
        url           = "https://localhost:9090"
        basicAuth     = true
        basicAuthUser = "admin"
        isDefault     = true
        jsonData = {
          httpMethod    = "POST"
          tlsSkipVerify = true
        }
        secureJsonData = {
          basicAuthPassword = var.prometheus_admin_password
        }
      }]
    })
  }
  grafana_dashboard_provider = {
    env    = "DASHBOARD_PROVIDER"
    "path" = "${local.grafana_dir}/provisioning/dashboards/default.yaml"
    content = yamlencode({
      apiVersion = 1
      providers = [{
        name    = "Default"
        folder  = "Provisioned"
        type    = "file"
        options = { "path" = "${local.grafana_dir}/provisioning/dashboards" }
      }]
    })
  }
  grafana_dashboard = {
    env     = "DASHBOARD"
    "path"  = "${local.grafana_dir}/provisioning/dashboards/irn.json"
    content = file("${path.module}/dashboard.json")
  }
  grafana_container_definition = {
    name : local.grafana_container_name,
    image : var.grafana_image,
    environment : [
      { name : local.grafana_tls_cert.env, value : local.grafana_tls_cert.content },
      { name : local.grafana_tls_key.env, value : local.grafana_tls_key.content },
      { name : local.grafana_datasources.env, value : local.grafana_datasources.content },
      { name : local.grafana_dashboard_provider.env, value : local.grafana_dashboard_provider.content },
      { name : local.grafana_dashboard.env, value : local.grafana_dashboard.content },
      { name : "GF_PATHS_DATA", value : local.grafana_dir },
      { name : "GF_PATHS_PROVISIONING", value : "${local.grafana_dir}/provisioning" },
      { name : "GF_SERVER_HTTP_PORT", value : tostring(var.grafana_port) },
      { name : "GF_SERVER_CERT_FILE", value : local.grafana_tls_cert.path },
      { name : "GF_SERVER_CERT_KEY", value : local.grafana_tls_key.path },
      { name : "GF_SERVER_PROTOCOL", value : "https" },
      { name : "GF_SECURITY_ADMIN_PASSWORD", value : var.grafana_admin_password },
    ]

    user : "1001",
    entryPoint : ["/bin/sh"],
    command : [
      "-c",
      <<-CMD
        set -e && \
        mkdir -p /data/grafana/provisioning/datasources && \
        mkdir -p /data/grafana/provisioning/dashboards && \
        printenv ${local.grafana_tls_cert.env} > ${local.grafana_tls_cert.path} && \
        printenv ${local.grafana_tls_key.env} > ${local.grafana_tls_key.path} && \
        printenv ${local.grafana_datasources.env} > ${local.grafana_datasources.path} && \
        printenv ${local.grafana_dashboard_provider.env} > ${local.grafana_dashboard_provider.path} && \
        printenv ${local.grafana_dashboard.env} > ${local.grafana_dashboard.path} && \
        exec /run.sh
      CMD
    ]


    portMappings : [
      {
        hostPort : var.grafana_port,
        protocol : "tcp",
        containerPort : var.grafana_port
      }
    ],

    mountPoints = [{
      containerPath = "/data"
      sourceVolume  = "data"
    }]

    logConfiguration : {
      logDriver : "awslogs",
      options : {
        awslogs-create-group : "True",
        awslogs-group : "/ecs/${local.name}-grafana",
        awslogs-region : "${var.region}",
        awslogs-stream-prefix : "ecs"
      }
    }
  }
}

resource "aws_ecs_task_definition" "this" {
  family = local.name
  container_definitions = jsonencode(concat(
    [local.irn_container_definition],
    var.enable_otel_collector ? [local.otel_container_definition] : [],
    var.enable_prometheus ? [local.prometheus_container_definition] : [],
    var.enable_grafana ? [local.grafana_container_definition] : []
  ))

  volume {
    name      = "data"
    host_path = "/mnt/irn-data"
  }

  network_mode = "host"

  memory             = var.node_memory
  cpu                = var.node_cpu
  execution_role_arn = var.ecs_task_execution_role_arn
  task_role_arn      = var.ecs_task_role_arn

  runtime_platform {
    operating_system_family = "LINUX"
  }

  tags = local.tags
}

resource "aws_ecs_service" "this" {
  name = local.name

  cluster                = aws_ecs_cluster.this.id
  task_definition        = aws_ecs_task_definition.this.arn
  propagate_tags         = "TASK_DEFINITION"
  enable_execute_command = true

  desired_count                      = 1
  deployment_minimum_healthy_percent = 0
  deployment_maximum_percent         = 100

  wait_for_steady_state = true

  depends_on = [
    aws_subnet.this,
    aws_route_table_association.this,
    aws_eip_association.this,
    aws_cloudwatch_log_group.this,
    aws_ebs_volume.this,
    aws_volume_attachment.this,
    aws_instance.this,
    aws_ecs_cluster.this,
  ]

  tags = local.tags
}

resource "terraform_data" "decommission_guard" {
  input = {
    region       = var.region
    cluster_name = aws_ecs_cluster.this.name

    # This safety switch is going to prevent unintentional node decommissions.
    # If you intend to decommission a node set `decommision_safety_switch` variable to `false`, and don't forget to enable it back when you're done.
    safety_switch = coalesce(var.decommission_safety_switch, true)
  }

  provisioner "local-exec" {
    when    = destroy
    command = "bash ${path.module}/decommission.sh ${self.input.region} ${self.input.cluster_name} ${self.input.safety_switch}"
  }

  depends_on = [aws_ecs_service.this]
}
