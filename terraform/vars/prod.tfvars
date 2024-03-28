irn_node_ec2_instance_type = "c5a.2xlarge" # 8vCPU / 16GiB
irn_node_cpu               = 8192
irn_node_memory            = 16384 - 1024 # Reserve some for the OS
irn_node_ebs_volume_size   = 100          # GiB
irn_node_log_level         = "warn,relay_irn=info,relay_rocks=info,network=info"
