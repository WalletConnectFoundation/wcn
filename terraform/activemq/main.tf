locals {
  name_prefix            = replace("${var.environment}-${var.app_name}-${var.broker_name}", "_", "-")
  master_username        = "relay"
  master_password        = data.aws_secretsmanager_secret_version.master_password.secret_string
  engine_version         = "5.17.6"
  queue_max_redeliveries = 3
  queue_redelivery_delay = 15000
}

data "aws_secretsmanager_secret" "master_password" {
  arn = aws_secretsmanager_secret.master_password.arn
}

data "aws_secretsmanager_secret_version" "master_password" {
  secret_id = data.aws_secretsmanager_secret.master_password.arn
}

resource "random_password" "master_password" {
  length  = 16
  special = false
}

resource "aws_secretsmanager_secret" "master_password" {
  name = "${local.name_prefix}-master-password"
}

resource "aws_secretsmanager_secret_version" "master_password" {
  secret_id     = aws_secretsmanager_secret.master_password.id
  secret_string = random_password.master_password.result
}

resource "aws_mq_broker" "mq_primary" {
  broker_name = "${local.name_prefix}-primary"

  engine_type             = "ActiveMQ"
  engine_version          = local.engine_version
  host_instance_type      = var.primary_instance_class
  deployment_mode         = "ACTIVE_STANDBY_MULTI_AZ"
  publicly_accessible     = false
  subnet_ids              = [var.private_subnet_ids[0], var.private_subnet_ids[1]]
  authentication_strategy = "simple"
  apply_immediately       = var.apply_immediately

  logs {
    audit   = var.enable_logs
    general = var.enable_logs
  }

  configuration {
    id       = aws_mq_configuration.mq_primary.id
    revision = aws_mq_configuration.mq_primary.latest_revision
  }

  user {
    username = local.master_username
    password = local.master_password
  }

  security_groups = [
    aws_security_group.service_security_group.id
  ]
}

resource "aws_mq_configuration" "mq_primary" {
  name = "${local.name_prefix}-primary-config"

  engine_type             = "ActiveMQ"
  engine_version          = local.engine_version
  authentication_strategy = "simple"

  data = <<DATA
<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<broker schedulePeriodForDestinationPurge="10000" schedulerSupport="true"
    xmlns="http://activemq.apache.org/schema/core">
    <destinationPolicy>
        <policyMap>
            <policyEntries>
                <policyEntry gcInactiveDestinations="true" inactiveTimoutBeforeGC="600000" topic="&gt;">
                    <pendingMessageLimitStrategy>
                        <constantPendingMessageLimitStrategy limit="1000"/>
                    </pendingMessageLimitStrategy>
                </policyEntry>
                <policyEntry gcInactiveDestinations="true" inactiveTimoutBeforeGC="600000" queue="&gt;"/>
            </policyEntries>
        </policyMap>
    </destinationPolicy>
    <plugins>
        <discardingDLQBrokerPlugin dropAll="true" dropTemporaryQueues="true" dropTemporaryTopics="true"/>
        <redeliveryPlugin fallbackToDeadLetter="true" sendToDlqIfMaxRetriesExceeded="true">
            <redeliveryPolicyMap>
                <redeliveryPolicyMap>
                    <redeliveryPolicyEntries/>
                    <defaultEntry>
                        <redeliveryPolicy initialRedeliveryDelay="${local.queue_redelivery_delay}" maximumRedeliveries="${local.queue_max_redeliveries}" redeliveryDelay="${local.queue_redelivery_delay}"/>
                    </defaultEntry>
                </redeliveryPolicyMap>
            </redeliveryPolicyMap>
        </redeliveryPlugin>
        <statisticsBrokerPlugin/>
    </plugins>
</broker>
DATA
}

resource "aws_security_group" "service_security_group" {
  name        = "${local.name_prefix}-service"
  description = "Allow ingress from the application"
  vpc_id      = var.vpc_id

  ingress {
    from_port   = 5671
    to_port     = 5671
    protocol    = "TCP"
    cidr_blocks = var.allowed_ingress_cidr_blocks
  }

  egress {
    from_port   = 0             # Allowing any incoming port
    to_port     = 0             # Allowing any outgoing port
    protocol    = "-1"          # Allowing any outgoing protocol
    cidr_blocks = ["0.0.0.0/0"] # Allowing traffic out to all IP addresses
  }
}
