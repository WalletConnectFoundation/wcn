local grafana = import '../../grafonnet-lib/grafana.libsonnet';
local panels = grafana.panels;
local targets = grafana.targets;

local defaults = import '../defaults.libsonnet';

local _configuration = defaults.configuration.timeseries_resource
                       .withUnit('ms');

{
  new(ds, vars)::
    panels.timeseries(
      title='RPC Task Duration by Task',
      datasource=ds.prometheus,
    )
    .configure(_configuration)

    .addTarget(targets.prometheus(
      datasource=ds.prometheus,
      expr='max by (task_name) (rate(rpc_task_duration_sum{}[1m])) / max by (task_name) (rate(rpc_task_duration_count{}[1m]))',
      refId='maxTaskDuration',
      legendFormat='max({{task_name}})',
      exemplar=true,
    ))

    .addTarget(targets.prometheus(
      datasource=ds.prometheus,
      expr='avg by (task_name) (rate(rpc_task_duration_sum{}[1m])) / avg by (task_name) (rate(rpc_task_duration_count{}[1m]))',
      refId='avgTaskDuration',
      legendFormat='avg({{task_name}})',
      exemplar=true,
    )),
}
