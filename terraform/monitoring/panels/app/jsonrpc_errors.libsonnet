local grafana = import '../../grafonnet-lib/grafana.libsonnet';
local panels = grafana.panels;
local targets = grafana.targets;

local defaults = import '../defaults.libsonnet';

{
  new(ds, vars)::
    panels.timeseries(
      title='JSON-RPC errors',
      datasource=ds.prometheus,
    )
    .configure(defaults.configuration.timeseries_tr80)

    .addTarget(targets.prometheus(
      datasource=ds.prometheus,
      expr='sum by (code) (rate(jsonrpc_errors_total{}[5m])) or vector(0)',
      legendFormat='{{code}}',
      exemplar=true,
    )),
}
