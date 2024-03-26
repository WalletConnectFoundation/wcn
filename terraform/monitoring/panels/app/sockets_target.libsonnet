local grafana = import '../../grafonnet-lib/grafana.libsonnet';
local panels = grafana.panels;
local targets = grafana.targets;

local defaults = import '../defaults.libsonnet';

{
  new(region, ds, vars):: self[region](ds, vars),
} +

{
  eu(ds, vars)::
    panels.timeseries(
      title='Sockets/Target EU',
      datasource=ds.prometheus,
    )
    .configure(defaults.configuration.timeseries_tr80)
    .addTarget(targets.prometheus(
      datasource=ds.prometheus,
      expr='avg_over_time(websocket_sockets_active{aws_ecs_task_family="%s_eu-central-1_relay"}[5m])' % vars.environment,
      legendFormat='{{cloud_availability_zone}} / r{{aws_ecs_task_revision}}',
      exemplar=true,
    )),
} +

{
  us(ds, vars)::
    panels.timeseries(
      title='Sockets/Target US',
      datasource=ds.prometheus,
    )
    .configure(defaults.configuration.timeseries_tr80)
    .addTarget(targets.prometheus(
      datasource=ds.prometheus,
      expr='avg_over_time(websocket_sockets_active{aws_ecs_task_family="%s_us-east-1_relay"}[5m])' % vars.environment,
      legendFormat='{{cloud_availability_zone}} / r{{aws_ecs_task_revision}}',
      exemplar=true,
    )),
} +

{
  ap(ds, vars)::
    panels.timeseries(
      title='Sockets/Target AP',
      datasource=ds.prometheus,
    )
    .configure(defaults.configuration.timeseries_tr80)
    .addTarget(targets.prometheus(
      datasource=ds.prometheus,
      expr='avg_over_time(websocket_sockets_active{aws_ecs_task_family="%s_ap-southeast-1_relay"}[5m])' % vars.environment,
      legendFormat='{{cloud_availability_zone}} / r{{aws_ecs_task_revision}}',
      exemplar=true,
    )),
}
