local grafana = import '../grafonnet-lib/grafana.libsonnet';
local defaults = import '../panels/defaults.libsonnet';

{
  local panel(ctx, namespace, title, expressionFn, alertFn = null) = grafana.panels
    .timeseries(title = title, datasource = ctx.cloudwatch)
    .configure(defaults.configuration.timeseries)
    .addTargets([
      grafana.targets.cloudwatch(
        datasource=ctx.cloudwatch,
        namespace=namespace,
        metricEditorMode = 1, // code
        expression=expressionFn(node),
        region=node.region,
        alias=node.id,
        refId=node.id,
      ) for node in ctx.nodes
    ]) + { [if alertFn != null && ctx.environment == 'prod' then 'alert']: alertFn(ctx, namespace, title) },

  local alertFn(evaluatorType, value) = function(ctx, namespace, title) grafana.alert.new(
    namespace='IRN %s %s' % [ctx.environment, namespace],
    name=title,
    message = '%s %d' % [evaluatorType, value],
    period='5m',
    frequency='1m',
    notifications=ctx.notifications,
    conditions=[
      grafana.alertCondition.new(
        evaluatorParams=[value],
        evaluatorType=evaluatorType,
        operatorType='or',
        queryRefId=node.id,
      ) for node in ctx.nodes]
  ),

  ecs:: {
    local new(ctx, metricName, statistic, alertFn = null) = panel(ctx, 'AWS/ECS', '%s %s' % [metricName, statistic], function(node)
      "SUM(SEARCH('{AWS/ECS, ClusterName, ServiceName} MetricName=\"%s\" ClusterName=\"%s\" ServiceName=\"%s\"', '%s'))" 
        % [metricName, node.ecs_cluster_name, node.ecs_service_name, statistic], alertFn,
    ).withUnit('percent'),

    cpu_min(ctx):: new(ctx, 'CPUUtilization', 'Minimum'),
    cpu_avg(ctx):: new(ctx, 'CPUUtilization', 'Average'),
    cpu_max(ctx):: new(ctx, 'CPUUtilization', 'Maximum', alertFn = alertFn('gt', 85)),

    memory_min(ctx):: new(ctx, 'MemoryUtilization', 'Minimum'),
    memory_avg(ctx):: new(ctx, 'MemoryUtilization', 'Average'),
    memory_max(ctx):: new(ctx, 'MemoryUtilization', 'Maximum', alertFn = alertFn('gt', 85)),
  },

  ec2:: {
    local new(ctx, title, expressionFn, alertFn = null) = panel(ctx, 'AWS/EC2', title, expressionFn, alertFn),

    local search(node, metricName, statistic) =
      "SUM(SEARCH('{AWS/EC2, InstanceId} MetricName=\"%s\" InstanceId=\"%s\"', '%s'))" 
        % [metricName, node.ec2_instance_id, statistic],

    local cpu(ctx, statistic, alertFn = null) =
      local metricName = 'CPUUtilization'; 
      new(ctx, '%s %s' % [metricName, statistic], function(node) search(node, metricName, statistic), alertFn = alertFn)
        .withUnit('percent'),

    cpu_min(ctx):: cpu(ctx, 'Minimum'),
    cpu_avg(ctx):: cpu(ctx, 'Average'),
    cpu_max(ctx):: cpu(ctx, 'Maximum', alertFn = alertFn('gt', 95)),

    local network(ctx, title, metricName, statistic, unit = null) =
      new(ctx, title, function(node) search(node, metricName, statistic) + ' / 300')
        .withUnit(unit),

    network_in(ctx):: network(ctx, 'NetworkIn bytes/sec', 'NetworkIn', 'Sum', 'Bps'),
    network_out(ctx):: network(ctx,'NetworkOut bytes/sec', 'NetworkOut', 'Sum', 'Bps'),

    network_packets_in(ctx):: network(ctx, 'NetworkPacketsIn/sec', 'NetworkPacketsIn', 'Sum'),
    network_packets_out(ctx):: network(ctx, 'NetworkPacketsOut/sec', 'NetworkPacketsOut', 'Sum'),
  },

  ebs:: {
    local new(ctx, title, unit, expressionFn, alertFn = null) = 
      panel(ctx, 'AWS/EBS', title, function(node) expressionFn(node.ebs_volume_id), alertFn)
        .withUnit(unit),

    local search(volumeId, metricName, statistic)
      = "SUM(SEARCH('{AWS/EBS, VolumeId} MetricName=\"%s\" VolumeId=\"%s\"', '%s'))"
          % [metricName, volumeId, statistic],

    read_throughput(ctx):: new(ctx, 'Read Throughput', 'Bps', function(volumeId)
      search(volumeId, 'VolumeReadBytes', 'Sum') + ' / 60'
    ),

    write_throughput(ctx):: new(ctx, 'Write Throughput', 'Bps', function(volumeId)
      search(volumeId, 'VolumeWriteBytes', 'Sum') + ' / 60'
    ),

    throughput(ctx):: new(ctx, 'Throughput', 'Bps', function(volumeId)
      '(%s + %s) / 60' % [search(volumeId, 'VolumeReadBytes', 'Sum'), search(volumeId, 'VolumeWriteBytes', 'Sum')],
      alertFn = alertFn('gt', 80 * 1000 * 1000), // 80 MB/s, default EBS limit is 125 MB/s
    ),

    read_ops(ctx):: new(ctx, 'Read Operations / sec', null, function(volumeId) 
      search(volumeId, 'VolumeReadOps', 'Sum') + ' / 60'
    ),

    write_ops(ctx):: new(ctx, 'Write Operations / sec', null, function(volumeId) 
      search(volumeId, 'VolumeWriteOps', 'Sum') + ' / 60'
    ),

    iops(ctx):: new(ctx, 'IO Operations / sec', null, function(volumeId) 
      '(%s + %s) / 60' % [search(volumeId,'VolumeReadOps', 'Sum'), search(volumeId, 'VolumeWriteOps', 'Sum')],
      alertFn = alertFn('gt', 1500), // default EBS limit is 3000
    ),

    queue_length(ctx):: new(ctx, 'Average Queue Length', null, function(volumeId) 
      search(volumeId, 'VolumeQueueLength', 'Average')
    ),

    idle_time(ctx):: new(ctx, 'Idle Time', 'percent', function(volumeId) 
      '%s / 60 * 100' % search(volumeId, 'VolumeIdleTime', 'Sum')
    ),

    local avg(a, b) = '%s / IF(%s != 0, %s, 1)' % [a, b, b],

    avg_read_size(ctx):: new(ctx, 'Average Read Size', 'bytes', function(volumeId) 
      avg(search(volumeId, 'VolumeReadBytes', 'Sum'), search(volumeId, 'VolumeReadOps', 'Sum'))
    ),

    avg_write_size(ctx):: new(ctx, 'Average Write Size', 'bytes', function(volumeId) 
      avg(search(volumeId, 'VolumeWriteBytes', 'Sum'), search(volumeId, 'VolumeWriteOps', 'Sum'))
    ),

    avg_read_latency(ctx):: new(ctx, 'Average Read Latency', 's', function(volumeId) 
      avg(search(volumeId, 'VolumeTotalReadTime', 'Sum'), search(volumeId, 'VolumeReadOps', 'Sum'))
    ),

    avg_write_latency(ctx):: new(ctx, 'Average Write Latency', 's', function(volumeId) 
      avg(search(volumeId, 'VolumeTotalWriteTime', 'Sum'), search(volumeId, 'VolumeWriteOps', 'Sum'))
    ),
  }
}
