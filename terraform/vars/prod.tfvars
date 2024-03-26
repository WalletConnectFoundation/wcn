autoscaling_max_instances               = 15
autoscaling_min_instances               = 2
project_data_cache_ttl                  = 300
registry_api_endpoint                   = "https://explorer-api.walletconnect.org"
disconnect_timeout_ms                   = 5 * 60 * 1000 # 5 mins
heartbeat_interval_ms                   = 30 * 1000
heartbeat_timeout_ms                    = 30 * 1000
webhook_activemq_primary_instance_class = "mq.m5.large"
relay_node_cpu                          = 4096
relay_node_memory                       = 8192
relay_log_level                         = "warn,relay=info"
relay_telemetry_level                   = "warn,relay=trace"
relay_telemetry_sample_ratio            = 0.05
storage_replication_lag_eu_ms           = 50
storage_replication_lag_ap_ms           = 700
storage_replication_lag_us_ms           = 450
irn_node_ec2_instance_type              = "c5a.2xlarge" # 8vCPU / 16GiB
irn_node_cpu                            = 8192
irn_node_memory                         = 16384 - 1024 # Reserve some for the OS
irn_node_ebs_volume_size                = 100          # GiB
irn_node_log_level                      = "warn,relay_irn=info,relay_rocks=info,network=info"

relay_lb_count = 4
# We have WebSocket session duration of up to 10m, and we want the clients to gradually migrate
# to the new target group to mitigate the database usage spikes.
#
# The downside is that this makes deployments longer.
# Only makes sense for prod.
relay_lb_deregistration_delay_secs = 600

data_lake_bucket_name    = "walletconnect.data-lake.prod"
data_lake_bucket_key_arn = "arn:aws:kms:eu-central-1:898587786287:key/06e7c9fd-943d-47bf-bcf4-781b44411ba4"

