local grafana = import '../../grafonnet-lib/grafana.libsonnet';
local panels = grafana.panels;
local targets = grafana.targets;

local defaults = import '../defaults.libsonnet';

{
  new(ds, vars)::
    panels.timeseries(
      title='Load Test Connected Clients',
      datasource=ds.cloudwatch.eu,
    )
    .configure(defaults.configuration.timeseries_tr80)

    .addTarget(targets.cloudwatch(
      alias='Successfully Paired Clients (Sum)',
      datasource=ds.cloudwatch.eu,
      dimensions={
        Target: 'wss://%s' % vars.fqdn,
      },
      matchExact=true,
      metricName='Pairing.connect.successful',
      namespace='%s_LoadTest_SignClient' % vars.environment,
      statistic='Sum',
    ))
    .addTarget(targets.cloudwatch(
      alias='Failed to Pair (Sum)',
      datasource=ds.cloudwatch.eu,
      dimensions={
        Target: 'wss://%s' % vars.fqdn,
      },
      matchExact=true,
      metricName='Pairing.connect.failed',
      namespace='%s_LoadTest_SignClient' % vars.environment,
      statistic='Sum',
    )),
}
