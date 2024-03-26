local grafana = import '../../grafonnet-lib/grafana.libsonnet';
local panels = grafana.panels;
local targets = grafana.targets;

local defaults = import '../defaults.libsonnet';

local _configuration = defaults.configuration.timeseries
                       .withThresholdStyle('area')
                       .withUnit('ms')
                       .withSpanNulls(true)
                       .addThreshold({
  color: 'red',
  value: 4000,
});

{
  new(ds, vars)::
    panels.timeseries(
      title='Auth Canary Latency',
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
      metricName='HappyPath.auth.latency',
      namespace='%s_Canary_AuthClient' % vars.environment,
      statistic='Average',
    )),
}
