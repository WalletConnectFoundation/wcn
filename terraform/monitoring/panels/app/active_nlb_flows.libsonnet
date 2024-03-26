local grafana = import '../../grafonnet-lib/grafana.libsonnet';
local panels = grafana.panels;
local targets = grafana.targets;

local defaults = import '../defaults.libsonnet';

local _configuration = defaults.configuration.timeseries
                       .withSoftLimit(
  axisSoftMin=0
);

{
  new(region, ds, vars):: self[region](ds, vars),
} +

{
  eu(ds, vars)::
    panels.timeseries(
      title='Active NLB Flows EU',
      datasource=ds.cloudwatch.eu,
    )
    .configure(_configuration)
    .addTargets([
      targets.cloudwatch(
        alias='LB-' + i,
        datasource=ds.cloudwatch.eu,
        dimensions={
          LoadBalancer: vars.load_balancers.eu[i],
        },
        matchExact=true,
        metricName='ActiveFlowCount_TLS',
        namespace='AWS/NetworkELB',
        statistic='Maximum',
      ) for i in std.range(0, std.length(vars.load_balancers.eu) - 1) ]      
    ),
} +

{
  us(ds, vars)::
    panels.timeseries(
      title='Active NLB Flows US',
      datasource=ds.cloudwatch.us,
    )
    .configure(_configuration)
    .addTargets([
      targets.cloudwatch(
        alias='LB-' + i,
        datasource=ds.cloudwatch.us,
        dimensions={
          LoadBalancer: vars.load_balancers.us[i],
        },
        matchExact=true,
        metricName='ActiveFlowCount_TLS',
        namespace='AWS/NetworkELB',
        statistic='Maximum',
      ) for i in std.range(0, std.length(vars.load_balancers.us) - 1) ]      
    ),
} +

{
  ap(ds, vars)::
    panels.timeseries(
      title='Active NLB Flows AP',
      datasource=ds.cloudwatch.ap,
    )
    .configure(_configuration)
    .addTargets([
      targets.cloudwatch(
        alias='LB-' + i,
        datasource=ds.cloudwatch.ap,
        dimensions={
          LoadBalancer: vars.load_balancers.ap[i],
        },
        matchExact=true,
        metricName='ActiveFlowCount_TLS',
        namespace='AWS/NetworkELB',
        statistic='Maximum',
      ) for i in std.range(0, std.length(vars.load_balancers.ap) - 1) ]      
    ),
}
