local grafana = import '../../grafonnet-lib/grafana.libsonnet';
local panels = grafana.panels;
local targets = grafana.targets;
local alert = grafana.alert;
local alertCondition = grafana.alertCondition;

local defaults = import '../defaults.libsonnet';

local _configuration = defaults.configuration.timeseries
                       .withSpanNulls(true)
                       .addOverride(grafana.override.new(
  name='Success',
  properties=[{
    id: 'color',
    value: {
      mode: 'fixed',
      fixedColor: 'green',
    },
  }],
))
                       .addOverride(grafana.override.new(
  name='Failure',
  properties=[{
    id: 'color',
    value: {
      mode: 'fixed',
      fixedColor: 'red',
    },
  }],
));

local canary_alert(region, tag, vars, minSuccessfulCanaryRuns, period) = alert.new(
  namespace='Relay',
  name='Relay %s - %s Canary (%s) alert' % [vars.environment, region, tag],
  message='Relay %s - %s Canary Failures' % [vars.environment, region],
  period=period,
  frequency='1m',
  notifications=vars.notifications,
  alertRuleTags={
    og_priority: 'P2',
  },
  conditions=[
    alertCondition.new(
      evaluatorParams=[minSuccessfulCanaryRuns],
      evaluatorType='lt',
      operatorType='or',
      queryRefId='Success',
      queryTimeStart=period,
      queryTimeEnd='now',
      reducerType='sum',
    ),
    alertCondition.new(
      evaluatorParams=[1],
      evaluatorType='gt',
      operatorType='or',
      queryRefId='Failure',
      queryTimeStart=period,
      queryTimeEnd='now',
      reducerType='sum',
    ),
  ]
);

{
  new(region, tag, ds, vars):: self[region](tag, ds, vars),
} +

{
  root(tag, ds, vars)::
    panels.timeseries(
      title='Canary (%s) - Root' % tag,
      datasource=ds.cloudwatch.eu,
    )
    .configure(_configuration)
    .setAlert(canary_alert('ROOT', tag, vars, 4, '5m'))

    .addTarget(targets.cloudwatch(
      alias='Success',
      datasource=ds.cloudwatch.eu,
      namespace='%s_Canary_SignClient' % vars.environment,
      metricName='HappyPath.connects.success',
      dimensions={
        Region: '*',
        Target: 'wss://%s' % vars.fqdn,
        Tag: tag,
      },
      matchExact=true,
      statistic='Sum',
      refId='Success',
    ))
    .addTarget(targets.cloudwatch(
      alias='Failures',
      datasource=ds.cloudwatch.eu,
      dimensions={
        Region: '*',
        Target: 'wss://%s' % vars.fqdn,
        Tag: tag,
      },
      matchExact=true,
      namespace='%s_Canary_SignClient' % vars.environment,
      metricName='HappyPath.connects.failure',
      statistic='Sum',
      refId='Failure',
    )),
} +

{
  eu(tag, ds, vars)::
    panels.timeseries(
      title='Canary (%s) - EU' % tag,
      datasource=ds.cloudwatch.eu,
    )
    .configure(_configuration)
    .setAlert(canary_alert('EU', tag, vars, 1, '8m'))

    .addTarget(targets.cloudwatch(
      alias='Success',
      datasource=ds.cloudwatch.eu,
      namespace='%s_Canary_SignClient' % vars.environment,
      metricName='HappyPath.connects.success',
      dimensions={
        Region: '*',
        Target: 'wss://%s' % vars.regionalized_fqdn.eu,
        Tag: tag,
      },
      matchExact=true,
      statistic='Sum',
      refId='Success',
    ))
    .addTarget(targets.cloudwatch(
      alias='Failures',
      datasource=ds.cloudwatch.eu,
      dimensions={
        Region: '*',
        Target: 'wss://%s' % vars.regionalized_fqdn.eu,
        Tag: tag,
      },
      matchExact=true,
      namespace='%s_Canary_SignClient' % vars.environment,
      metricName='HappyPath.connects.failure',
      statistic='Sum',
      refId='Failure',
    )),
} +

{
  us(tag, ds, vars)::
    panels.timeseries(
      title='Canary (%s) - US' % tag,
      datasource=ds.cloudwatch.eu,
    )
    .configure(_configuration)
    .setAlert(canary_alert('US', tag, vars, 1, '8m'))

    .addTarget(targets.cloudwatch(
      alias='Success',
      datasource=ds.cloudwatch.eu,
      dimensions={
        Region: '*',
        Target: 'wss://%s' % vars.regionalized_fqdn.us,
        Tag: tag,
      },
      matchExact=true,
      namespace='%s_Canary_SignClient' % vars.environment,
      metricName='HappyPath.connects.success',
      statistic='Sum',
      refId='Success',
    ))
    .addTarget(targets.cloudwatch(
      alias='Failures',
      datasource=ds.cloudwatch.eu,
      dimensions={
        Region: '*',
        Target: 'wss://%s' % vars.regionalized_fqdn.us,
        Tag: tag,
      },
      matchExact=true,
      namespace='%s_Canary_SignClient' % vars.environment,
      metricName='HappyPath.connects.failure',
      statistic='Sum',
      refId='Failure',
    )),
} +

{
  ap(tag, ds, vars)::
    panels.timeseries(
      title='Canary (%s) - AP' % tag,
      datasource=ds.cloudwatch.eu,
    )
    .configure(_configuration)
    .setAlert(canary_alert('AP', tag, vars, 1, '8m'))

    .addTarget(targets.cloudwatch(
      alias='Success',
      datasource=ds.cloudwatch.eu,
      dimensions={
        Region: '*',
        Target: 'wss://%s' % vars.regionalized_fqdn.ap,
        Tag: tag,
      },
      matchExact=true,
      metricName='HappyPath.connects.success',
      namespace='%s_Canary_SignClient' % vars.environment,
      statistic='Sum',
      refId='Success',
    ))
    .addTarget(targets.cloudwatch(
      alias='Failures',
      datasource=ds.cloudwatch.eu,
      dimensions={
        Region: '*',
        Target: 'wss://%s' % vars.regionalized_fqdn.ap,
        Tag: tag,
      },
      matchExact=true,
      metricName='HappyPath.connects.failure',
      namespace='%s_Canary_SignClient' % vars.environment,
      statistic='Sum',
      refId='Failure',
    )),
}
