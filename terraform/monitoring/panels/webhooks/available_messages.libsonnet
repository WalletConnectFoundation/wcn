local grafana = import '../../grafonnet-lib/grafana.libsonnet';
local panels = grafana.panels;
local targets = grafana.targets;

local defaults = import '../defaults.libsonnet';

{
  new(ds, vars)::
    panels.timeseries(
      title='Available Messages (Queue Depth)',
      datasource=ds.cloudwatch.eu,
    )
    .configure(defaults.configuration.timeseries_tr80)

    .addTarget(targets.cloudwatch(
      alias='MQ-1',
      datasource=ds.cloudwatch.eu,
      dimensions={
        Broker: '%s-relay-webhook-activemq-primary-1' % vars.environment,
      },
      matchExact=true,
      metricName='TotalMessageCount',
      namespace='AWS/AmazonMQ',
      statistic='Maximum',
    ))
    .addTarget(targets.cloudwatch(
      alias='MQ-2',
      datasource=ds.cloudwatch.eu,
      dimensions={
        Broker: '%s-relay-webhook-activemq-primary-2' % vars.environment,
      },
      matchExact=true,
      metricName='TotalMessageCount',
      namespace='AWS/AmazonMQ',
      statistic='Maximum',
    )),

}
