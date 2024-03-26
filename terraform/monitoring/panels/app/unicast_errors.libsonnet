local grafana = import '../../grafonnet-lib/grafana.libsonnet';
local panels = grafana.panels;
local targets = grafana.targets;

local defaults = import '../defaults.libsonnet';

{
  new(ds, vars)::
    panels.timeseries(
      title='Unicast Errors',
      datasource=ds.prometheus,
    )
    .configure(defaults.configuration.timeseries_tr80)

    .addTarget(targets.prometheus(
      datasource=ds.prometheus,
      expr='sum(rate(http_requests_finished_total{route_name="unicast", status_code=~"5.+"}[5m])) or vector(0)',
      legendFormat='server errors',
      exemplar=true,
    )),
}
