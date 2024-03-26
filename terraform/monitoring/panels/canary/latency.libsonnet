local grafana = import '../../grafonnet-lib/grafana.libsonnet';
local panels = grafana.panels;
local targets = grafana.targets;
local alert = grafana.alert;
local alertCondition = grafana.alertCondition;

local defaults = import '../defaults.libsonnet';

local max_latency = 10000;  // 10 seconds

local _configuration = defaults.configuration.timeseries
                       .withUnit('ms')
                       .withSpanNulls(true)
                       .addThreshold({
  color: 'red',
  value: max_latency,
});

local canary_alert(tag, vars) = alert.new(
  namespace='Relay',
  name='Relay %s - Canary (%s) Latency alert' % [vars.environment, tag],
  message='Relay %s - Canary Latency alert' % vars.environment,
  period='5m',
  frequency='1m',
  noDataState='no_data',
  notifications=vars.notifications,
  alertRuleTags={
    og_priority: 'P3',
  },
  conditions=[
    alertCondition.new(
      evaluatorParams=[max_latency],
      evaluatorType='gt',
      operatorType='and',
      queryRefId='Latency',
      queryTimeStart='5m',
      queryTimeEnd='now',
      reducerType='avg',
    ),
  ]
);

{
  new(tag, ds, vars)::
    panels.timeseries(
      title='Canary (%s) Latency' % tag,
      datasource=ds.cloudwatch.eu,
    )
    .configure(_configuration)
    .setAlert(canary_alert(tag, vars))

    .addTarget(targets.cloudwatch(
      datasource=ds.cloudwatch.eu,
      dimensions={
        Region: '*',
        Target: '*',
        Tag: tag,
      },
      matchExact=true,
      metricName='HappyPath.connects.latency',
      namespace='%s_Canary_SignClient' % vars.environment,
      statistic='Average',
      refId='Latency',
    )),
}
