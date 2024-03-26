local grafana = import '../../grafonnet-lib/grafana.libsonnet';
local panels = grafana.panels;
local targets = grafana.targets;

local defaults = import '../defaults.libsonnet';

local _configuration = defaults.configuration.timeseries_tr80
                       .withUnit('percent')
                       .withSoftLimit(
  axisSoftMin=80,
  axisSoftMax=100,
);

{
  new(ds, vars)::
    panels.timeseries(
      title='Project Cache Hit Ratio',
      datasource=ds.prometheus,
    )
    .configure(_configuration)

    .addTarget(targets.prometheus(
      datasource=ds.prometheus,
      expr='(1 - sum(rate(project_data_api_requests_total[1m])) / sum(rate(project_data_requests_total[1m]))) * 100',
      legendFormat='Cache Hits %',
      refId='CacheHitRatio',
    )),
}
