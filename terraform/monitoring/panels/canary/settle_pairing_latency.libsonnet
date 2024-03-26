local grafana = import '../../grafonnet-lib/grafana.libsonnet';
local panels = grafana.panels;
local targets = grafana.targets;

local defaults = import '../defaults.libsonnet';

local _configuration = defaults.configuration.timeseries
                       .withUnit('ms')
                       .withSpanNulls(true)
                       .addThreshold({
  color: 'red',
  value: 1200,
});

{
  new(tag, ds, vars)::
    panels.timeseries(
      title='Canary (%s) Settle Pairing Latency' % tag,
      datasource=ds.cloudwatch.eu,
    )
    .configure(_configuration)

    .addTarget(targets.cloudwatch(
      datasource=ds.cloudwatch.eu,
      dimensions={
        Region: '*',
        Target: '*',
        Tag: tag,
      },
      matchExact=true,
      metricName='HappyPath.connects.settlePairingLatency',
      namespace='%s_Canary_SignClient' % vars.environment,
      statistic='Average',
    )),
}
