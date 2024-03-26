local grafana = import '../../grafonnet-lib/grafana.libsonnet';
local panels = grafana.panels;
local targets = grafana.targets;
local alert = grafana.alert;
local alertCondition = grafana.alertCondition;

local defaults = import '../defaults.libsonnet';

local _configuration = defaults.configuration.timeseries_resource
                       .withUnit('percent')
                       .withSoftLimit(
  axisSoftMin=0,
  axisSoftMax=30,
);

local cpu_alert(vars) = alert.new(
  namespace='Relay',
  name='Relay %s - App CPU/Memory alert' % vars.environment,
  message='Relay %s - App CPU/Memory' % vars.environment,
  period='25m',
  frequency='1m',
  notifications=vars.notifications,
  alertRuleTags={
    og_priority: 'P3',
  },

  conditions=[
    alertCondition.new(
      evaluatorParams=[50],
      evaluatorType='gt',
      operatorType='or',
      queryRefId='CPU_Avg',
      queryTimeStart='25m',
      reducerType='max',
    ),
    alertCondition.new(
      evaluatorParams=[50],
      evaluatorType='gt',
      operatorType='or',
      queryRefId='Mem_Avg',
      queryTimeStart='25m',
      reducerType='max',
    ),
  ]
);

{
  new(region, ds, vars):: self[region](ds, vars),
}

{
  eu(ds, vars)::
    panels.timeseries(
      title='App CPU/Memory EU',
      datasource=ds.cloudwatch.eu,
    )
    .configure(_configuration)
    .setAlert(cpu_alert(vars))

    .addTarget(targets.cloudwatch(
      alias='CPU (Max)',
      datasource=ds.cloudwatch.eu,
      dimensions={
        ServiceName: vars.ecs_service_names.eu,
      },
      metricName='CPUUtilization',
      namespace='AWS/ECS',
      statistic='Maximum',
      refId='CPU_Max',
    ))
    .addTarget(targets.cloudwatch(
      alias='CPU (Avg)',
      datasource=ds.cloudwatch.eu,
      dimensions={
        ServiceName: vars.ecs_service_names.eu,
      },
      metricName='CPUUtilization',
      namespace='AWS/ECS',
      statistic='Average',
      refId='CPU_Avg',
    ))

    .addTarget(targets.cloudwatch(
      alias='Memory (Max)',
      datasource=ds.cloudwatch.eu,
      dimensions={
        ServiceName: vars.ecs_service_names.eu,
      },
      metricName='MemoryUtilization',
      namespace='AWS/ECS',
      statistic='Maximum',
      refId='Mem_Max',
    ))
    .addTarget(targets.cloudwatch(
      alias='Memory (Avg)',
      datasource=ds.cloudwatch.eu,
      dimensions={
        ServiceName: vars.ecs_service_names.eu,
      },
      metricName='MemoryUtilization',
      namespace='AWS/ECS',
      statistic='Average',
      refId='Mem_Avg',
    )),
} +

{
  us(ds, vars)::
    panels.timeseries(
      title='App CPU/Memory US',
      datasource=ds.cloudwatch.us,
    )
    .configure(_configuration)
    .setAlert(cpu_alert(vars))

    .addTarget(targets.cloudwatch(
      alias='CPU (Max)',
      datasource=ds.cloudwatch.us,
      dimensions={
        ServiceName: vars.ecs_service_names.us,
      },
      metricName='CPUUtilization',
      namespace='AWS/ECS',
      statistic='Maximum',
      refId='CPU_Max',
    ))
    .addTarget(targets.cloudwatch(
      alias='CPU (Avg)',
      datasource=ds.cloudwatch.us,
      dimensions={
        ServiceName: vars.ecs_service_names.us,
      },
      metricName='CPUUtilization',
      namespace='AWS/ECS',
      statistic='Average',
      refId='CPU_Avg',
    ))

    .addTarget(targets.cloudwatch(
      alias='Memory (Max)',
      datasource=ds.cloudwatch.us,
      dimensions={
        ServiceName: vars.ecs_service_names.us,
      },
      metricName='MemoryUtilization',
      namespace='AWS/ECS',
      statistic='Maximum',
      refId='Mem_Max',
    ))
    .addTarget(targets.cloudwatch(
      alias='Memory (Avg)',
      datasource=ds.cloudwatch.us,
      dimensions={
        ServiceName: vars.ecs_service_names.us,
      },
      metricName='MemoryUtilization',
      namespace='AWS/ECS',
      statistic='Average',
      refId='Mem_Avg',
    )),
} +

{
  ap(ds, vars)::
    panels.timeseries(
      title='App CPU/Memory AP',
      datasource=ds.cloudwatch.ap,
    )
    .configure(_configuration)
    .setAlert(cpu_alert(vars))

    .addTarget(targets.cloudwatch(
      alias='CPU (Max)',
      datasource=ds.cloudwatch.ap,
      dimensions={
        ServiceName: vars.ecs_service_names.ap,
      },
      metricName='CPUUtilization',
      namespace='AWS/ECS',
      statistic='Maximum',
      refId='CPU_Max',
    ))
    .addTarget(targets.cloudwatch(
      alias='CPU (Avg)',
      datasource=ds.cloudwatch.ap,
      dimensions={
        ServiceName: vars.ecs_service_names.ap,
      },
      metricName='CPUUtilization',
      namespace='AWS/ECS',
      statistic='Average',
      refId='CPU_Avg',
    ))

    .addTarget(targets.cloudwatch(
      alias='Memory (Max)',
      datasource=ds.cloudwatch.ap,
      dimensions={
        ServiceName: vars.ecs_service_names.ap,
      },
      metricName='MemoryUtilization',
      namespace='AWS/ECS',
      statistic='Maximum',
      refId='Mem_Max',
    ))
    .addTarget(targets.cloudwatch(
      alias='Memory (Avg)',
      datasource=ds.cloudwatch.ap,
      dimensions={
        ServiceName: vars.ecs_service_names.ap,
      },
      metricName='MemoryUtilization',
      namespace='AWS/ECS',
      statistic='Average',
      refId='Mem_Avg',
    )),
}
