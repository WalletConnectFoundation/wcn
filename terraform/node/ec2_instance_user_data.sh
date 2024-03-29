Content-Type: multipart/mixed; boundary="//"
MIME-Version: 1.0

--//
Content-Type: text/cloud-config; charset="us-ascii"
MIME-Version: 1.0
Content-Transfer-Encoding: 7bit
Content-Disposition: attachment; filename="cloud-config.txt"

#cloud-config
cloud_final_modules:
- [scripts-user, always]

--//
Content-Type: text/x-shellscript; charset="us-ascii"
MIME-Version: 1.0
Content-Transfer-Encoding: 7bit
Content-Disposition: attachment; filename="userdata.txt"

#!/bin/bash

# This script is being called on creation and every reboot of our EC2 instances.
# Multipart magic is required in order to execute it on reboots, and not only on creation.
#
# See https://repost.aws/knowledge-center/execute-user-data-ec2

rm -f /etc/ecs/ecs.config

# Assign this EC2 instance to our ECS cluster
echo ECS_CLUSTER=${ecs_cluster} >> /etc/ecs/ecs.config

# We are increasing stop timeout here, because only one node is allowed to 
# restart at a time, and other ones are waiting for their turn.
echo ECS_CONTAINER_STOP_TIMEOUT=1h >> /etc/ecs/ecs.config

# Install Intance Connect package in order to be able to the instance without SSH key configuration
# directly from AWS console.
# This package is being installed by default on Amazon Linux 2023, but not on ECS optimized one (!).  
dnf install -y ec2-instance-connect

# In the following section we tweak various kernel networking settings.
# See https://www.kernel.org/doc/Documentation for more details.

rm -f /etc/sysctl.conf

# General socket configuration (applies to both TCP and UDP)
# -----------------------------------------------------------------------------

# The default setting of the socket send buffer in bytes.
echo net.core.wmem_default="8388608" >> /etc/sysctl.conf # 8MiB

# The maximum send socket buffer size in bytes.
echo net.core.wmem_max="134217728" >> /etc/sysctl.conf # 128MiB

# The default setting of the socket receive buffer in bytes.
echo net.core.rmem_default="8388608" >> /etc/sysctl.conf # 8MiB

# The maximum receive socket buffer size in bytes.
echo net.core.rmem_max="134217728" >> /etc/sysctl.conf # 128MiB

# UDP configuration
# -----------------------------------------------------------------------------

# vector of 3 INTEGERs: min, pressure, max
#
# Number of pages allowed for queueing by all UDP sockets.
#
# min: Below this number of pages UDP is not bothered about its
# memory appetite. When amount of memory allocated by UDP exceeds
# this number, UDP starts to moderate memory usage.
#
# pressure: This value was introduced to follow format of tcp_mem.
#
# max: Number of pages allowed for queueing by all UDP sockets.
#
# Default is calculated at boot time from amount of available memory.
echo net.ipv4.udp_mem="65536 131072 262144" >> /etc/sysctl.conf

# Minimal size of send buffer used by UDP sockets in moderation.
#
# Each UDP socket is able to use the size for sending data, even if
# total pages of UDP sockets exceed udp_mem pressure. The unit is byte.
#
# Default: 4K
echo net.ipv4.udp_wmem_min="1048576" >> /etc/sysctl.conf # 1MiB

# Minimal size of receive buffer used by UDP sockets in moderation.
#
# Each UDP socket is able to use the size for receiving data, even if
# total pages of UDP sockets exceed udp_mem pressure. The unit is byte.
# 
# Default: 4K
echo net.ipv4.udp_rmem_min="1048576" >> /etc/sysctl.conf # 1MiB

# TCP configuration
# -----------------------------------------------------------------------------

# Enable window scaling as defined in RFC1323.
echo net.ipv4.tcp_window_scaling=1 >> /etc/sysctl.conf

# Apply settings without reboot
sysctl -p


# Busy-wait until IRN data EBS storage is attached
while true
do
  # The device may not always be named `xvdb`, sometimes (!) it's `nvme`.
  # However, fortunately aws at least creates a symlink in `/dev`.
  if [ $(ls /dev | grep -c "xvdb") -eq 1 ]
  then
      # Make XFS file system, no-op if it already exists
      mkfs.xfs /dev/xvdb
    
      mkdir /mnt/irn-data
      mount /dev/xvdb /mnt/irn-data
      chown 1001:1001 /mnt/irn-data
      break
  fi

  sleep 1
done

--//--
