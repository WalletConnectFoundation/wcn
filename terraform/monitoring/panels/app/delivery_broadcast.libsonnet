local grafana = import '../../grafonnet-lib/grafana.libsonnet';
local panels = grafana.panels;
local targets = grafana.targets;

local defaults = import '../defaults.libsonnet';

{
  new(ds, vars)::
    panels.timeseries(
      title='Delivery Broadcast (via PubSub)',
      datasource=ds.prometheus,
    )
    .configure(defaults.configuration.timeseries_tr80)

    .addTarget(targets.prometheus(
      datasource=ds.prometheus,
      expr='sum(increase(rpc_publish_gossip_notified_total{}[5m]))',
      legendFormat='Notified (x-region)',
      exemplar=true,
    ))

    .addTarget(targets.prometheus(
      datasource=ds.prometheus,
      expr='sum(increase(rpc_publish_gossip_dispatched_total{}[5m]))',
      legendFormat='Dispatched (x-region)',
      exemplar=true,
    ))

    .addTarget(targets.prometheus(
      datasource=ds.prometheus,
      expr='sum(increase(rpc_publish_gossip_received_total{}[5m]))',
      legendFormat='Received (x-region)',
      exemplar=true,
    ))

    .addTarget(targets.prometheus(
      datasource=ds.prometheus,
      expr='sum(increase(rpc_publish_failed_total{}[5m]))',
      legendFormat='Failed (x-region)',
      exemplar=true,
    ))

    .addTarget(targets.prometheus(
      datasource=ds.prometheus,
      expr='sum(increase(rpc_publish_mailbox_delivery_total{command_source="mailbox_gossip"}[5m]))',
      legendFormat='Delivered (x-region)',
      exemplar=true,
    )),
}
