local grafana = import '../../grafonnet-lib/grafana.libsonnet';
local panels = grafana.panels;
local targets = grafana.targets;
local fieldConfig = grafana.fieldConfig;

local defaults = import '../defaults.libsonnet';

{
  new(ds, vars)::
    panels.stat(
      title='Max Concurrent Sockets',
      datasource=ds.prometheus,
      fieldConfig=fieldConfig.new(
        defaults=fieldConfig.new_default(
          color={
            mode: fieldConfig.fieldColorModeId.thresholds,
          },
        ),
      ),
    )
    .setOptions(
      reduceOptions={
        calcs: ['max'],
        fields: '',
        values: false,
      },
    )
    .addTarget(targets.prometheus(
      datasource=ds.prometheus,
      expr='max(sum(websocket_sockets_active))',
      exemplar=true,
    )),
}
