local grafana = import '../../grafonnet-lib/grafana.libsonnet';
local panels = grafana.panels;
local targets = grafana.targets;

local defaults = import '../defaults.libsonnet';

{
  new(ds, vars)::
    panels.timeseries(
      title='Mailbox Messages',
      datasource=ds.prometheus,
    )
    .configure(defaults.configuration.timeseries_tr80)

    .addTarget(targets.prometheus(
      datasource=ds.prometheus,
      expr='sum(rate(mailbox_messages_stored_total{mailbox_kind="ephemeral"}[5m]))',
      legendFormat='Ephemeral Messages Stored',
      exemplar=true,
    ))

    .addTarget(targets.prometheus(
      datasource=ds.prometheus,
      expr='sum(rate(mailbox_messages_removed_total{mailbox_kind="ephemeral"}[5m]))',
      legendFormat='Ephemeral Messages Delivered',
      exemplar=true,
    ))

    .addTarget(targets.prometheus(
      datasource=ds.prometheus,
      expr='sum(rate(mailbox_messages_stored_total{mailbox_kind="persistent"}[5m]))',
      legendFormat='Persistent Messages Stored',
      exemplar=true,
    ))

    .addTarget(targets.prometheus(
      datasource=ds.prometheus,
      expr='sum(rate(mailbox_messages_removed_total{mailbox_kind="persistent"}[5m]))',
      legendFormat='Persistent Messages Delivered',
      exemplar=true,
    )),
}
