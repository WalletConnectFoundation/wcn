local grafana = import '../../grafonnet-lib/grafana.libsonnet';
local panels = grafana.panels;
local targets = grafana.targets;

local defaults = import '../defaults.libsonnet';

local _configuration = defaults.configuration.timeseries
                       .addOverride(grafana.override.new(
  name='Heap',
  properties=[{
    id: 'color',
    value: {
      mode: 'fixed',
      fixedColor: 'purple',
    },
  }],
))
                       .withThresholdStyle('area')
                       .setThresholds(
  baseColor='green',
  steps=[
    { value: 80, color: 'red' },
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
      title='Heap Usage',
      description='The percentage of the ActiveMQ JVM memory limit that the broker currently uses.',
      datasource=ds.cloudwatch.eu,
    )
    .configure(_configuration)

    .addTarget(targets.cloudwatch(
      alias='Mem',
      datasource=ds.cloudwatch.eu,
      dimensions={
        Broker: '%s-relay-webhook-activemq-primary-1' % vars.environment,
      },
      matchExact=true,
      namespace='AWS/AmazonMQ',
      metricName='HeapUsage',
      statistic='Average',
      refId='Heap',
    )),

}
