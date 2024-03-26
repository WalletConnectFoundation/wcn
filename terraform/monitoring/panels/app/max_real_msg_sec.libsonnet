local grafana = import '../../grafonnet-lib/grafana.libsonnet';
local panels = grafana.panels;
local targets = grafana.targets;
local fieldConfig = grafana.fieldConfig;

local defaults = import '../defaults.libsonnet';

{
  new(ds, vars)::
    panels.stat(
      title='Max Real msg/sec',
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
      expr='max_over_time( sum(rate(websocket_messages_total{class="data"}[2m]))[$__range:1m] )',
      exemplar=true,
    )),
}
