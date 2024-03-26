local grafana = import '../../grafonnet-lib/grafana.libsonnet';
local panels = grafana.panels;
local targets = grafana.targets;
local alert = grafana.alert;
local alertCondition = grafana.alertCondition;

local defaults = import '../defaults.libsonnet';

local _configuration = defaults.configuration.timeseries_resource
                       .withUnit('ms');

local task_duration_alert(vars) = alert.new(
  namespace='Relay',
  name='Relay - RPC Task Duration alert',
  message='Relay - RPC Task Duration alert',
  period='5m',
  frequency='1m',
  notifications=vars.notifications,
  alertRuleTags={
    og_priority: 'P3',
  },

  conditions=[
    alertCondition.new(
      evaluatorParams=[60000],
      evaluatorType='gt',
      operatorType='or',
      queryRefId='maxTaskDuration',
      queryTimeStart='5m',
      reducerType='max',
    ),
  ]
);

{
  new(ds, vars)::
    panels.timeseries(
      title='RPC Task Duration by Region',
      datasource=ds.prometheus,
    )
    .configure(_configuration)

    .addTarget(targets.prometheus(
      datasource=ds.prometheus,
      expr='max by (aws_ecs_task_family) (rate(rpc_task_duration_sum{}[1m])) / max by (aws_ecs_task_family) (rate(rpc_task_duration_count{}[1m]))',
      refId='maxTaskDuration',
      legendFormat='max({{aws_ecs_task_family}})',
      exemplar=true,
    ))

    .addTarget(targets.prometheus(
      datasource=ds.prometheus,
      expr='avg by (aws_ecs_task_family) (rate(rpc_task_duration_sum{}[1m])) / avg by (aws_ecs_task_family) (rate(rpc_task_duration_count{}[1m]))',
      refId='avgTaskDuration',
      legendFormat='avg({{aws_ecs_task_family}})',
      exemplar=true,
    )),
}
