local grafana = import '../grafonnet-lib/grafana.libsonnet';
local defaults = import '../panels/defaults.libsonnet';

{
  local new(ctx, title, queries) = 
    local hasAlert = std.any(std.map(function(q) std.objectHas(q, 'alert'), queries));
    grafana.panels
      .timeseries(title = title, datasource = ctx.prometheus)
      .configure(defaults.configuration.timeseries)
      .addTargets([
        grafana.targets.prometheus(
          datasource=ctx.prometheus,
          expr = queries[idx].expr,
          legendFormat = queries[idx].legendFormat,
          refId='Q' + idx,
        ) for idx in std.range(0, std.length(queries) - 1)
      ]) + { 
        [if hasAlert && ctx.environment == 'prod' then 'alert']: grafana.alert.new(
          namespace='IRN ' + ctx.environment,
          name=title,
          message = title,
          period='5m',
          frequency='1m',
          notifications=ctx.notifications,
          conditions=std.filterMap(
            function(idx) std.objectHas(queries[idx], 'alert'),
            function(idx) grafana.alertCondition.new(
              evaluatorParams=[queries[idx].alert.value],
              evaluatorType=queries[idx].alert.evaluatorType,
              reducerType=queries[idx].alert.reducerType,
              operatorType='or',
              queryRefId='Q' + idx,
            ),
            std.range(0, std.length(queries) - 1),
          ), 
        ) 
      },

  local alert(evaluatorType, value, reducerType = 'avg') = {
    evaluatorType: evaluatorType, value: value, reducerType: reducerType, 
  },

  client:: {
    operations(ctx):: new(ctx, 'Operations', [
      { expr: 'sum(rate(irn_api_client_operation_results_total{client_tag=""}[1m]))', legendFormat: 'total' },
      { expr: 'sum(rate(irn_api_client_operation_results_total{client_tag="shadowing"}[1m]))', legendFormat: 'shadowing total' },
      { expr: 'sum(rate(irn_api_client_operation_results_total[1m])) by(operation, client_tag)', legendFormat: '{{operation}} {{client_tag}}' },
    ]),

    latency(ctx):: new(ctx, 'Latency', [{
      expr: 'sum(rate(irn_api_client_operation_duration_sum{completed="true"}[1m])) by(task_name, cloud_region, client_tag) / sum(rate(irn_api_client_operation_duration_count{completed="true"}[1m])) by(task_name, cloud_region, client_tag)',
      legendFormat: '{{cloud_region}} {{task_name}} {{client_tag}}',
      alert: alert('gt', 200, reducerType = 'min'), 
    }])
    .withUnit('ms'),

    success_rate(ctx):: new(ctx, 'Success Rate', [{
      expr: 'sum(rate(irn_api_client_operation_results_total{err="",client_tag=""}[1m])) / sum(rate(irn_api_client_operation_results_total{client_tag=""}[1m]))',
      legendFormat: 'success rate',
      alert: alert('lt', 0.97),
    },{
      expr: 'sum(rate(irn_api_client_operation_results_total{err="",client_tag="shadowing"}[1m])) / sum(rate(irn_api_client_operation_results_total{client_tag="shadowing"}[1m]))',
      legendFormat: 'shadowing success rate',
    }])
    .withUnit('percentunit')
    .withSoftLimit(axisSoftMin=0.95, axisSoftMax=1),
  
    errors(ctx):: new(ctx, 'Errors', [{
      expr: 'sum(rate(irn_api_client_operation_results_total{err!=""}[1m])) by(operation,err,client_tag)',
      legendFormat: '{{operation}} {{err}} {{client_tag}}',
    }]),
  },

  internals:: {
    local taskDuration(ctx, title, metric, attributes, legendFormat) = new(ctx, title, [{
      expr: 'rate(%s_sum{%s}[1m]) / rate(%s_count{%s}[1m])' % [metric, attributes, metric, attributes],
      legendFormat: legendFormat,
    }])
    .withUnit('ms'),

    local network_requests(ctx, title, attributes) = new(ctx, title, [
      { expr: 'avg(rate(irn_rpc_started_total{%s}[1m])) by(kind,name)' % attributes, legendFormat: '{{kind}}: {{name}}' },
      { expr: 'avg(sum(rate(irn_rpc_started_total{%s}[1m])) by(aws_ecs_task_family))' % attributes, legendFormat: 'total' },
    ]),

    request_permits(ctx):: new(ctx, 'Request Permits', [{
      expr: 'irn_network_request_permits_available',
      legendFormat: '{{aws_ecs_task_family}}',
      alert: alert('lt', 1000),
    }]),

    request_queue_permits(ctx):: new(ctx, 'Request Queue Permits', [
      { expr: 'irn_network_request_queue_permits_available', legendFormat: '{{aws_ecs_task_family}}' },
    ]),

    request_queue_timeout(ctx):: new(ctx, 'Request Queue Timeout', [
      { expr: 'rate(irn_network_request_queue_timeout_total[1m])', legendFormat: '{{aws_ecs_task_family}}' },
    ]),

    storage_api_requests_per_second(ctx):: new(ctx, 'Storage API: Requests per Second', [
      { expr: 'avg(rate(irn_api_storage_task_started_total[1m])) by(task_name)', legendFormat: '{{task_name}}' },
      { expr: 'sum(irn_api_storage_task_started_total) - sum(irn_api_storage_task_finished_total)', legendFormat: 'concurrency' },
      { expr: 'avg(sum(rate(irn_api_storage_task_started_total[1m])) by(aws_ecs_task_family))', legendFormat: 'total (avg)' },
      { expr: 'sum(rate(irn_api_storage_task_started_total[1m]))', legendFormat: 'total (sum)' },
    ]),

    storage_api_request_total_duration(ctx):: 
      taskDuration(ctx, 'Storage API: Request Total Duration', 'irn_api_storage_task_duration', '', '{{task_name}}'),

    network_requests_total_inbound_per_second(ctx)::
        network_requests(ctx, 'Network Requests: Total Inbound per Second (avg)', 'task_name="inbound"'),

    network_requests_total_outbound_per_second(ctx)::
        network_requests(ctx, 'Network Requests: Total Outbound per Second (avg)', 'task_name="outbound"'),

    network_requests_inbound_poll_duration(ctx):: taskDuration(
      ctx, 'Network Requests: Inbound (Poll Duration)', 'irn_rpc_poll_duration', 'task_name="inbound"', '{{kind}}: {{name}}',
    ),

    network_requests_outbound_poll_duration(ctx):: taskDuration(
      ctx, 'Network Requests: Outbound (Poll Duration)', 'irn_rpc_poll_duration', 'task_name="outbound"', '{{kind}}: {{name}}',
    ),

    network_requests_inbound_total_duration(ctx):: taskDuration(
      ctx, 'Network Requests: Inbound (Total Duration)', 'irn_rpc_duration', 'task_name="inbound"', '{{kind}}: {{name}}',
    ),

    network_requests_outbound_total_duration(ctx):: taskDuration(
      ctx, 'Network Requests: Outbound (Total Duration)', 'irn_rpc_duration', 'task_name="outbound"', '{{kind}}: {{name}}',
    ),
      
    local_storage_operations(ctx):: new(ctx, 'Local Storage Operations (avg)', [{ 
      expr: 'avg(rate(irn_storage_task_duration_sum[1m]) / rate(irn_storage_task_duration_count{completed="true"}[1m])) by(task_name)',
      legendFormat: '{{task_name}}',
    }])
    .withUnit('ms'),

    total_disk_space(ctx):: new(ctx, 'Total Disk Space', [
      { expr: 'avg(irn_disk_total_space) by(aws_ecs_task_family)', legendFormat: '{{aws_ecs_task_family}}' },
    ])
    .withUnit('bytes'),

    available_disk_space(ctx):: new(ctx, 'Available Disk Space', [
      { expr: 'avg(irn_disk_available_space) by(aws_ecs_task_family)', legendFormat: '{{aws_ecs_task_family}}' },
    ])
    .withUnit('bytes'),

    disk_usage(ctx):: new(ctx, 'Disk Usage', [{
      expr: 'avg(1 - irn_disk_available_space / irn_disk_total_space) by(aws_ecs_task_family)',
      legendFormat: '{{aws_ecs_task_family}}',
      alert: alert('gt', 50) 
    }])
    .withUnit('percentunit'),
  }
}
