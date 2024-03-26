local grafana = import '../../grafonnet-lib/grafana.libsonnet';
local panels = grafana.panels;
local targets = grafana.targets;
local alert = grafana.alert;
local alertCondition = grafana.alertCondition;

local defaults = import '../defaults.libsonnet';

local canary_alert(vars) = alert.new(
  namespace='Relay',
  name='%s Chat Canary alert' % vars.environment,
  message='%s Chat Canary Failures' % vars.environment,
  period='5m',
  frequency='1m',
  notifications=vars.notifications,
  alertRuleTags={
    og_priority: 'P3',
  },
  conditions=[
    alertCondition.new(
      evaluatorParams=[4],
      evaluatorType='lt',
      operatorType='or',
      queryRefId='Success',
      queryTimeStart='5m',
      queryTimeEnd='now',
      reducerType='sum',
    ),
    alertCondition.new(
      evaluatorParams=[1],
      evaluatorType='gt',
      operatorType='or',
      queryRefId='Failure',
      queryTimeStart='5m',
      queryTimeEnd='now',
      reducerType='sum',
    ),
  ]
);

{
  new(ds, vars)::
    panels.timeseries(
      title='Chat Canary',
      datasource=ds.cloudwatch.eu,
    )
    .configure(defaults.configuration.timeseries_tr80)
    // .setAlert(canary_alert(vars))

    .addTarget(targets.cloudwatch(
      alias='Success',
      datasource=ds.cloudwatch.eu,
      dimensions={
        Region: '*',
        Target: 'wss://%s' % vars.fqdn,
      },
      matchExact=true,
      metricName='HappyPath.chat.success',
      namespace='%s_Canary_ChatClient' % vars.environment,
      statistic='Sum',
      refId='Success',
    ))
    .addTarget(targets.cloudwatch(
      alias='Failure',
      datasource=ds.cloudwatch.eu,
      dimensions={
        Region: '*',
        Target: 'wss://%s' % vars.fqdn,
      },
      matchExact=true,
      metricName='HappyPath.chat.failure',
      namespace='%s_Canary_ChatClient' % vars.environment,
      statistic='Sum',
      refId='Failure',
    )),
}
