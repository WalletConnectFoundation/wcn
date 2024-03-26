local grafana = import '../grafonnet-lib/grafana.libsonnet';
local dashboard = grafana.dashboard;
local layout = grafana.layout;

local height = 8;
local pos(h=height) = layout.pos(h);

local client = (import 'prometheus.libsonnet').client;
local internals = (import 'prometheus.libsonnet').internals;

local ecs = (import 'cloudwatch.libsonnet').ecs;
local ec2 = (import 'cloudwatch.libsonnet').ec2;
local ebs = (import 'cloudwatch.libsonnet').ebs;

local ctx = { 
  prometheus: {
    type: 'prometheus',
    uid: std.extVar('prometheus_uid'),
  },
  cloudwatch: {
    type: 'cloudwatch',
    uid: std.extVar('cloudwatch_uid'),
  },

  nodes: std.extVar('nodes'),

  notifications: std.extVar('notifications'),
  environment: std.extVar('environment'),
};

dashboard.new(
  title=std.extVar('dashboard_title'),
  uid=std.extVar('dashboard_uid'),
  editable=true,
  graphTooltip=dashboard.graphTooltips.sharedCrosshair,
)
.addAnnotation(
  grafana.annotation.new(
    target={
      limit: 100,
      matchAny: false,
      tags: [],
      type: 'dashboard',
    },
  )
)
.addPanels(
  layout.generate_grid([
    grafana.row.new(title='Client') { gridPos: pos(1).full },

    client.operations(ctx) { gridPos: pos().one_quarter },
    client.latency(ctx) { gridPos: pos().one_quarter },
    client.success_rate(ctx) { gridPos: pos().one_quarter },
    client.errors(ctx) { gridPos: pos().one_quarter },

    grafana.row.new(title='Internals') { gridPos: pos(1).full },

    internals.request_permits(ctx) { gridPos: pos().half },
    internals.request_queue_permits(ctx) { gridPos: pos().one_quarter },
    internals.request_queue_timeout(ctx) { gridPos: pos().one_quarter },

    internals.storage_api_requests_per_second(ctx) { gridPos: pos().half },
    internals.storage_api_request_total_duration(ctx) { gridPos: pos().half },

    internals.network_requests_total_inbound_per_second(ctx) { gridPos: pos().half },
    internals.network_requests_total_outbound_per_second(ctx) { gridPos: pos().half },

    internals.network_requests_inbound_poll_duration(ctx) { gridPos: pos().half },
    internals.network_requests_outbound_poll_duration(ctx) { gridPos: pos().half },
    internals.network_requests_inbound_total_duration(ctx) { gridPos: pos().half },
    internals.network_requests_outbound_total_duration(ctx) { gridPos: pos().half },

    internals.local_storage_operations(ctx) { gridPos: pos().full },

    internals.total_disk_space(ctx) { gridPos: pos().one_third },
    internals.available_disk_space(ctx) { gridPos: pos().one_third },
    internals.disk_usage(ctx) { gridPos: pos().one_third },

    grafana.row.new(title='ECS') { gridPos: pos(1).full },

    ecs.cpu_min(ctx) { gridPos: pos().one_third },
    ecs.cpu_avg(ctx) { gridPos: pos().one_third },
    ecs.cpu_max(ctx) { gridPos: pos().one_third },

    ecs.memory_min(ctx) { gridPos: pos().one_third },
    ecs.memory_avg(ctx) { gridPos: pos().one_third },
    ecs.memory_max(ctx) { gridPos: pos().one_third },

    grafana.row.new(title='EC2') { gridPos: pos(1).full },

    ec2.cpu_min(ctx) { gridPos: pos().one_third },
    ec2.cpu_avg(ctx) { gridPos: pos().one_third },
    ec2.cpu_max(ctx) { gridPos: pos().one_third },

    ec2.network_in(ctx) { gridPos: pos().half },
    ec2.network_out(ctx) { gridPos: pos().half },
    
    ec2.network_packets_in(ctx) { gridPos: pos().half },
    ec2.network_packets_out(ctx) { gridPos: pos().half },

    grafana.row.new(title='EBS') { gridPos: pos(1).full },

    ebs.throughput(ctx) { gridPos: pos().one_third },
    ebs.read_throughput(ctx) { gridPos: pos().one_third },
    ebs.write_throughput(ctx) { gridPos: pos().one_third },

    ebs.iops(ctx) { gridPos: pos().one_third },
    ebs.read_ops(ctx) { gridPos: pos().one_third },
    ebs.write_ops(ctx) { gridPos: pos().one_third },

    ebs.queue_length(ctx) { gridPos: pos().half },
    ebs.idle_time(ctx) { gridPos: pos().half },

    ebs.avg_read_size(ctx) { gridPos: pos().half },
    ebs.avg_write_size(ctx) { gridPos: pos().half },

    ebs.avg_read_latency(ctx) { gridPos: pos().half },
    ebs.avg_write_latency(ctx) { gridPos: pos().half },
  ])
)
