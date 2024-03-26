local grafana = import '../../grafonnet-lib/grafana.libsonnet';
local panels = grafana.panels;
local targets = grafana.targets;

local defaults = import '../defaults.libsonnet';

local _configuration = defaults.configuration.timeseries
                       .withSoftLimit(
  axisSoftMin=0,
  axisSoftMax=5,
);

{
  new(region, ds, vars):: self[region](ds, vars),
} +

{
  eu(ds, vars)::
    panels.timeseries(
      title='Healthy Hosts EU',
      datasource=ds.cloudwatch.eu,
    )
    .configure(_configuration)

    .addTarget(targets.cloudwatch(
      alias='eu-central-1',
      datasource=ds.cloudwatch.eu,
      metricQueryType=grafana.target.cloudwatch.metricQueryTypes.query,

      dimensions={
        TargetGroup: vars.target_groups.eu,
      },
      metricName='HealthyHostCount',
      namespace='AWS/NetworkELB',
      sql={
        from: {
          property: {
            name: 'AWS/NetworkELB',
            type: 'string',
          },
          type: 'property',
        },
        select: {
          name: 'MAX',
          parameters: [
            {
              name: 'HealthyHostCount',
              type: 'functionParameter',
            },
          ],
          type: 'function',
        },
        where: {
          expressions: [
            {
              operator: {
                name: '=',
                value: vars.load_balancers.eu[0],
              },
              property: {
                name: 'LoadBalancer',
                type: 'string',
              },
              type: 'operator',
            },
          ],
          type: 'and',
        },
      },
      sqlExpression="SELECT MAX(HealthyHostCount) FROM \"AWS/NetworkELB\" WHERE LoadBalancer = '%s'" % [vars.load_balancers.eu[0]],
      statistic='Maximum',
    )),
} +

{
  us(ds, vars)::
    panels.timeseries(
      title='Healthy Hosts US',
      datasource=ds.cloudwatch.us,
    )
    .configure(_configuration)

    .addTarget(targets.cloudwatch(
      alias='us-east-1',
      datasource=ds.cloudwatch.us,
      metricQueryType=grafana.target.cloudwatch.metricQueryTypes.query,

      dimensions={
        TargetGroup: vars.target_groups.us,
      },
      metricName='HealthyHostCount',
      namespace='AWS/NetworkELB',
      sql={
        from: {
          property: {
            name: 'AWS/NetworkELB',
            type: 'string',
          },
          type: 'property',
        },
        select: {
          name: 'MAX',
          parameters: [
            {
              name: 'HealthyHostCount',
              type: 'functionParameter',
            },
          ],
          type: 'function',
        },
        where: {
          expressions: [
            {
              operator: {
                name: '=',
                value: vars.load_balancers.us[0],
              },
              property: {
                name: 'LoadBalancer',
                type: 'string',
              },
              type: 'operator',
            },
          ],
          type: 'and',
        },
      },
      sqlExpression="SELECT MAX(HealthyHostCount) FROM \"AWS/NetworkELB\" WHERE LoadBalancer = '%s'" % [vars.load_balancers.us[0]],
      statistic='Maximum',
    )),
} +

{
  ap(ds, vars)::
    panels.timeseries(
      title='Healthy Hosts AP',
      datasource=ds.cloudwatch.ap,
    )
    .configure(_configuration)

    .addTarget(targets.cloudwatch(
      alias='ap-southeast-1',
      datasource=ds.cloudwatch.ap,
      metricQueryType=grafana.target.cloudwatch.metricQueryTypes.query,

      dimensions={
        TargetGroup: vars.target_groups.ap,
      },
      metricName='HealthyHostCount',
      namespace='AWS/NetworkELB',
      sql={
        from: {
          property: {
            name: 'AWS/NetworkELB',
            type: 'string',
          },
          type: 'property',
        },
        select: {
          name: 'MAX',
          parameters: [
            {
              name: 'HealthyHostCount',
              type: 'functionParameter',
            },
          ],
          type: 'function',
        },
        where: {
          expressions: [
            {
              operator: {
                name: '=',
                value: vars.load_balancers.ap[0],
              },
              property: {
                name: 'LoadBalancer',
                type: 'string',
              },
              type: 'operator',
            },
          ],
          type: 'and',
        },
      },
      sqlExpression="SELECT MAX(HealthyHostCount) FROM \"AWS/NetworkELB\" WHERE LoadBalancer = '%s'" % [vars.load_balancers.ap[0]],
      statistic='Maximum',
    )),
}
