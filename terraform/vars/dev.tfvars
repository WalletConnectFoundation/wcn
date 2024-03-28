irn_node_ec2_instance_type = "t2.small" # 1vCPU / 2GiB
irn_node_cpu               = 1024 - 256 # Istance may become unhealthy if CPU is overloaded
irn_node_memory            = 2048 - 256 # Reserve some for the OS
irn_node_ebs_volume_size   = 1          # GiB
irn_node_log_level         = "error,relay_irn=info,relay_rocks=info,network=info,irn_bin=info"
