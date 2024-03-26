local grafana = import '../../grafonnet-lib/grafana.libsonnet';
local panels = grafana.panels;
local targets = grafana.targets;

local defaults = import '../defaults.libsonnet';

{
  new(ds, vars)::
    panels.timeseries(
      title='Webhook Dispatch',
      datasource=ds.prometheus,
    )
    .configure(defaults.configuration.timeseries_tr80)

    .addTarget(targets.prometheus(
      datasource=ds.prometheus,
      expr='sum(rate(webhook_requests_queued_total[5m]))',
      legendFormat='Total Queued',
      exemplar=true,
    ))

    .addTarget(targets.prometheus(
      datasource=ds.prometheus,
      expr='sum(rate(webhook_requests_processed_total[5m]))',
      legendFormat='Total Processed',
      exemplar=true,
    ))

    .addTarget(targets.prometheus(
      datasource=ds.prometheus,
      expr='sum(rate(webhook_requests_processed_total{disposition="accepted"}[5m]))',
      legendFormat='Accepted',
      exemplar=true,
    ))

    .addTarget(targets.prometheus(
      datasource=ds.prometheus,
      expr='sum(rate(webhook_requests_processed_total{disposition="rejected"}[5m]))',
      legendFormat='Rejected',
      exemplar=true,
    )),
}
