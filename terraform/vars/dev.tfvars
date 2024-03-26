autoscaling_max_instances               = 2
autoscaling_min_instances               = 1
project_data_cache_ttl                  = 300
registry_api_endpoint                   = "https://explorer-api.walletconnect.org"
disconnect_timeout_ms                   = 60 * 1000
heartbeat_interval_ms                   = 15 * 1000
heartbeat_timeout_ms                    = 15 * 1000
webhook_activemq_primary_instance_class = "mq.t3.micro"
relay_node_cpu                          = 512
relay_node_memory                       = 1024
relay_log_level                         = "warn,relay=debug"
relay_telemetry_level                   = "warn,relay=trace"
relay_telemetry_sample_ratio            = 1.0
storage_replication_lag_eu_ms           = 50
storage_replication_lag_ap_ms           = 700
storage_replication_lag_us_ms           = 450
irn_node_ec2_instance_type              = "t2.small" # 1vCPU / 2GiB
irn_node_cpu                            = 1024 - 256 # Istance may become unhealthy if CPU is overloaded
irn_node_memory                         = 2048 - 256 # Reserve some for the OS
irn_node_ebs_volume_size                = 1          # GiB
irn_node_log_level                      = "error,relay_irn=info,relay_rocks=info,network=info,irn_bin=info"

data_lake_bucket_name    = "walletconnect.data-lake.dev"
data_lake_bucket_key_arn = "arn:aws:kms:eu-central-1:898587786287:key/745dcbdf-80cb-4550-b8a2-c80362427f51"

