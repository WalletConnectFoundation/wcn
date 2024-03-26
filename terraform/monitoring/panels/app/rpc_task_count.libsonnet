local grafana = import '../../grafonnet-lib/grafana.libsonnet';
local panels = grafana.panels;
local targets = grafana.targets;

local defaults = import '../defaults.libsonnet';

{
  new(ds, vars)::
    panels.timeseries(
      title='RPC Task Overview',
      datasource=ds.prometheus,
    )
    .configure(defaults.configuration.timeseries_tr80)

    .addTarget(targets.prometheus(
      datasource=ds.prometheus,
      expr='sum(rate(rpc_task_finished_total{}[1m]))',
      refId='finished',
      legendFormat='Finished Tasks',
      exemplar=true,
    ))

    .addTarget(targets.prometheus(
      datasource=ds.prometheus,
      expr='sum(rate(rpc_task_started_total{}[1m]))',
      refId='started',
      legendFormat='Started Tasks',
      exemplar=true,
    )),
}
