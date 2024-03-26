local grafana = import '../../grafonnet-lib/grafana.libsonnet';
local panels = grafana.panels;
local targets = grafana.targets;

local defaults = import '../defaults.libsonnet';

local _configuration = defaults.configuration.timeseries_tr80
                       .withUnit('ms');

{
  new(ds, vars)::
    panels.timeseries(
      title='Webhook Queue',
      datasource=ds.prometheus,
    )
    .configure(_configuration)

    .addTarget(targets.prometheus(
      datasource=ds.prometheus,
      expr='sum(rate(webhook_queue_time_sum[5m]))',
      refId='webhook_queue_time_sum',
      exemplar=true,
      hide=true,
    ))
    .addTarget(targets.prometheus(
      datasource=ds.prometheus,
      expr='sum(rate(webhook_queue_time_count[5m]))',
      refId='webhook_queue_time_count',
      exemplar=true,
      hide=true,
    ))
    .addTarget(targets.math(
      expr='$webhook_queue_time_sum / $webhook_queue_time_count',
      refId='Queue Time',
    ))

    .addTarget(targets.prometheus(
      datasource=ds.prometheus,
      expr='sum(rate(webhook_processing_time_sum[5m]))',
      refId='webhook_processing_time_sum',
      exemplar=true,
      hide=true,
    ))
    .addTarget(targets.prometheus(
      datasource=ds.prometheus,
      expr='sum(rate(webhook_processing_time_count[5m]))',
      refId='webhook_processing_time_count',
      exemplar=true,
      hide=true,
    ))
    .addTarget(targets.math(
      expr='$webhook_processing_time_sum / $webhook_processing_time_count',
      refId='Processing Time',
    )),
}
