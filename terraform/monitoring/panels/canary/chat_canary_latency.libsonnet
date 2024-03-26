local grafana = import '../../grafonnet-lib/grafana.libsonnet';
local panels = grafana.panels;
local targets = grafana.targets;

local defaults = import '../defaults.libsonnet';

local _configuration = defaults.configuration.timeseries_resource
                       .withUnit('ms');

{
  new(ds, vars)::
    panels.timeseries(
      title='Chat Canary Latency',
      datasource=ds.cloudwatch.eu,
    )
    .configure(_configuration)

    .addTarget(targets.cloudwatch(
      alias='Average Latency',
      datasource=ds.cloudwatch.eu,
      dimensions={
        Region: '*',
        Target: '*',
      },
      matchExact=true,
      metricName='HappyPath.chat.latency',
      namespace='%s_Canary_ChatClient' % vars.environment,
      statistic='Average',
    )),
}
