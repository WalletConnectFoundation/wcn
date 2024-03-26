local grafana = import '../../grafonnet-lib/grafana.libsonnet';
local panels = grafana.panels;
local targets = grafana.targets;

local defaults = import '../defaults.libsonnet';

local _configuration = defaults.configuration.timeseries
                       .setThresholds(
  baseColor='green',
  steps=[
    { value: 50, color: 'red' },
  ]
)
                       .withUnit('percent')
                       .withSoftLimit(
  axisSoftMin=0,
  axisSoftMax=100,
);

{
  new(ds, vars)::
    panels.timeseries(
      title='Store Usage',
      description='The percent used by the storage limit. If this reaches 100, the broker will refuse messages.',
      datasource=ds.cloudwatch.eu,
    )
    .configure(_configuration)

    .addTarget(targets.cloudwatch(
      alias='Store',
      datasource=ds.cloudwatch.eu,
      dimensions={
        Broker: '%s-relay-webhook-activemq-primary-1' % vars.environment,
      },
      matchExact=true,
      namespace='AWS/AmazonMQ',
      metricName='StorePercentUsage',
      statistic='Average',
    )),

}
