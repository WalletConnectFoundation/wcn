local grafana = import '../../grafonnet-lib/grafana.libsonnet';
local panels = grafana.panels;
local targets = grafana.targets;

local defaults = import '../defaults.libsonnet';

local _configuration = defaults.configuration.timeseries_resource
                       .withUnit('ms');

{
  new(ds, vars)::
    panels.timeseries(
      title='Load Test Latency',
      datasource=ds.cloudwatch.eu,
    )
    .configure(_configuration)

    .addTarget(targets.cloudwatch(
      alias='Pairing Latency',
      datasource=ds.cloudwatch.eu,
      dimensions={
        Target: 'wss://%s' % vars.fqdn,
      },
      matchExact=true,
      metricName='Pairing.latency',
      namespace='%s_LoadTest_SignClient' % vars.environment,
      statistic='Average',
    ))
    .addTarget(targets.cloudwatch(
      alias='Handshake Latency',
      datasource=ds.cloudwatch.eu,
      dimensions={
        Target: 'wss://%s' % vars.fqdn,
      },
      matchExact=true,
      metricName='Pairing.handshake.latency',
      namespace='%s_LoadTest_SignClient' % vars.environment,
      statistic='Average',
    )),
}
