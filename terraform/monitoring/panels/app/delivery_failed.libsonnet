local grafana = import '../../grafonnet-lib/grafana.libsonnet';
local panels = grafana.panels;
local targets = grafana.targets;

local defaults = import '../defaults.libsonnet';

{
  new(ds, vars)::
    panels.timeseries(
      title='Delivery Sources - Failed',
      datasource=ds.prometheus,
    )
    .configure(defaults.configuration.timeseries_tr80)

    .addTarget(targets.prometheus(
      datasource=ds.prometheus,
      expr='sum by (local, error, success) (increase(rpc_publish_realtime_delivery_total{success="false"}[5m]))',
      refId='sources',
      exemplar=true,
    )),
}
