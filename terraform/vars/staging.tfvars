irn_node_ec2_instance_type = "c5a.large" # 2vCPU / 4GiB (non-burstable)
irn_node_cpu               = 2048 - 256  # Istance may become unhealthy if CPU is overloaded
irn_node_memory            = 4096 - 1024 # Reserve some for the OS
irn_node_ebs_volume_size   = 30          # GiB
irn_node_log_level         = "error,relay_irn=info,relay_rocks=info,network=info,openraft=info"
