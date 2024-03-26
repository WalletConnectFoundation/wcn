local grafana = import '../../grafonnet-lib/grafana.libsonnet';
local panels = grafana.panels;
local targets = grafana.targets;
local alert = grafana.alert;
local alertCondition = grafana.alertCondition;

local defaults = import '../defaults.libsonnet';

local _configuration = defaults.configuration.timeseries
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

local canary_alert(vars) = alert.new(
  namespace='Relay',
  name='Relay %s - Auth Canary alert' % vars.environment,
  message='Relay %s - Auth Canary Failures' % vars.environment,
  period='5m',
  frequency='1m',
  notifications=vars.notifications,
  alertRuleTags={
    og_priority: 'P2',
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
      title='Auth Canary',
      datasource=ds.cloudwatch.eu,
    )
    .configure(_configuration)
    .setAlert(canary_alert(vars))

    .addTarget(targets.cloudwatch(
      alias='Success',
      datasource=ds.cloudwatch.eu,
      dimensions={
        Region: '*',
        Target: 'wss://%s' % vars.fqdn,
      },
      matchExact=true,
      metricName='HappyPath.auth.success',
      namespace='%s_Canary_AuthClient' % vars.environment,
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
      metricName='HappyPath.auth.failure',
      namespace='%s_Canary_AuthClient' % vars.environment,
      statistic='Sum',
      refId='Failure',
    )),
}
