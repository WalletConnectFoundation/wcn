local grafana = import '../../grafonnet-lib/grafana.libsonnet';
local panels = grafana.panels;
local targets = grafana.targets;

local defaults = import '../defaults.libsonnet';

local _configuration = defaults.configuration.timeseries_tr80
                       .withSoftLimit(
  axisSoftMin=0,
  axisSoftMax=250,
);


{
  new(region, ds, vars):: self[region](ds, vars),
} +

{
  eu(ds, vars)::
    panels.timeseries(
      title='NLB Target Resets EU',
      description='When the NLB has connection failures to the targets then these jump. We for instance had this when we had a too low file descriptor limit.',
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
        metricName='TCP_Target_Reset_Count',
        namespace='AWS/NetworkELB',
        statistic='Sum',
      ) for i in std.range(0, std.length(vars.load_balancers.eu) - 1) ]      
    ),
} +

{
  us(ds, vars)::
    panels.timeseries(
      title='NLB Target Resets US',
      description='When the NLB has connection failures to the targets then these jump. We for instance had this when we had a too low file descriptor limit.',
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
        metricName='TCP_Target_Reset_Count',
        namespace='AWS/NetworkELB',
        statistic='Sum',
      ) for i in std.range(0, std.length(vars.load_balancers.us) - 1) ]      
    ),
} +

{
  ap(ds, vars)::
    panels.timeseries(
      title='NLB Target Resets AP',
      description='When the NLB has connection failures to the targets then these jump. We for instance had this when we had a too low file descriptor limit.',
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
        metricName='TCP_Target_Reset_Count',
        namespace='AWS/NetworkELB',
        statistic='Sum',
      ) for i in std.range(0, std.length(vars.load_balancers.ap) - 1) ]      
    ),
}
