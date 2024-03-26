local grafana = import '../../grafonnet-lib/grafana.libsonnet';
local panels = grafana.panels;
local targets = grafana.targets;

local defaults = import '../defaults.libsonnet';

{
  new(ds, vars)::
    panels.timeseries(
      title='Active Sockets',
      datasource=ds.prometheus,
    )
    .configure(defaults.configuration.timeseries_tr80)

    .addTarget(targets.prometheus(
      datasource=ds.prometheus,
      expr='sum(avg_over_time(websocket_sockets_active{aws_ecs_task_family="%s_eu-central-1_relay"}[5m]))' % vars.environment,
      legendFormat='eu-central-1',
      exemplar=true,
    ))

    .addTarget(targets.prometheus(
      datasource=ds.prometheus,
      expr='sum(avg_over_time(websocket_sockets_active{aws_ecs_task_family="%s_us-east-1_relay"}[5m]))' % vars.environment,
      legendFormat='us-east-1',
      exemplar=true,
    ))

    .addTarget(targets.prometheus(
      datasource=ds.prometheus,
      expr='sum(avg_over_time(websocket_sockets_active{aws_ecs_task_family="%s_ap-southeast-1_relay"}[5m]))' % vars.environment,
      legendFormat='ap-southeast-1',
      exemplar=true,
    )),
}
