local grafana = import '../../grafonnet-lib/grafana.libsonnet';
local panels = grafana.panels;
local targets = grafana.targets;
local alert = grafana.alert;
local alertCondition = grafana.alertCondition;

local defaults = import '../defaults.libsonnet';

local cpu_alert(vars) = alert.new(
  namespace='Relay',
  name='Relay %s - Webhooks AMQP CPU alert' % vars.environment,
  message='Relay %s - Webhooks AMQP CPU alert' % vars.environment,
  period='5m',
  frequency='1m',
  notifications=vars.notifications,
  conditions=[
    alertCondition.new(
      evaluatorParams=[50],
      evaluatorType='gt',
      operatorType='or',
      queryRefId='CPU_Max',
      queryTimeStart='5m',
      queryTimeEnd='now',
      reducerType='avg',
    ),
  ]
);

{
  new(ds, vars)::
    panels.timeseries(
      title='CPU Utilization',
      description='The percentage of allocated Amazon EC2 compute units that the broker currently uses.',
      datasource=ds.cloudwatch.eu,
    )
    .configure(defaults.configuration.timeseries_resource)
    .setAlert(cpu_alert(vars))

    .addTarget(targets.cloudwatch(
      alias='CPU (Max)',
      datasource=ds.cloudwatch.eu,
      dimensions={
        Broker: '%s-relay-webhook-activemq-primary-1' % vars.environment,
      },
      matchExact=true,
      namespace='AWS/AmazonMQ',
      metricName='CpuUtilization',
      statistic='Maximum',
      refId='CPU_Max',
    ))
    .addTarget(targets.cloudwatch(
      alias='CPU (Avg)',
      datasource=ds.cloudwatch.eu,
      dimensions={
        Broker: '%s-relay-webhook-activemq-primary-1' % vars.environment,
      },
      matchExact=true,
      namespace='AWS/AmazonMQ',
      metricName='CpuUtilization',
      statistic='Average',
      refId='CPU_Avg',
    )),

}
