local grafana = import 'grafonnet-lib/grafana.libsonnet';
local panels = import 'panels/panels.libsonnet';

local dashboard = grafana.dashboard;
local annotation = grafana.annotation;
local layout = grafana.layout;

local ds = {
  prometheus: {
    type: 'prometheus',
    uid: std.extVar('prometheus_uid'),
  },
  cloudwatch: {
    eu: {
      type: 'cloudwatch',
      uid: std.extVar('cloudwatch_eu_uid'),
    },
    us: {
      type: 'cloudwatch',
      uid: std.extVar('cloudwatch_us_uid'),
    },
    ap: {
      type: 'cloudwatch',
      uid: std.extVar('cloudwatch_ap_uid'),
    },
  },
};
local vars = {
  fqdn: std.extVar('fqdn'),
  environment: std.extVar('environment'),
  notifications: std.parseJson(std.extVar('notifications')),

  regionalized_fqdn: {
    eu: std.extVar('regionalized_fqdn_eu'),
    us: std.extVar('regionalized_fqdn_us'),
    ap: std.extVar('regionalized_fqdn_ap'),
  },

  load_balancers: {
    eu: std.extVar('load_balancers_eu'),
    us: std.extVar('load_balancers_us'),
    ap: std.extVar('load_balancers_ap'),
  },

  target_groups: {
    eu: std.extVar('target_group_eu'),
    us: std.extVar('target_group_us'),
    ap: std.extVar('target_group_ap'),
  },

  ecs_service_names: {
    eu: std.extVar('ecs_service_name_eu'),
    us: std.extVar('ecs_service_name_us'),
    ap: std.extVar('ecs_service_name_ap'),
  },
};

////////////////////////////////////////////////////////////////////////////////

local height = 8;
local pos = {
  full: { w: layout.width.full, h: height },
  _2: { w: layout.width.half, h: height },
  _4: { w: layout.width.quarter, h: height },
  _3: { w: layout.width.one_third, h: height },
  _5: { w: layout.width.one_fifth, h: height },
  _6: { w: layout.width.one_sixth, h: height },

  title: { w: layout.width.full, h: height / 2 },
};

////////////////////////////////////////////////////////////////////////////////

dashboard.new(
  title=std.extVar('dashboard_title'),
  uid=std.extVar('dashboard_uid'),
  editable=true,
  graphTooltip=dashboard.graphTooltips.sharedCrosshair,
)
.addAnnotation(
  annotation.new(
    target={
      limit: 100,
      matchAny: false,
      tags: [],
      type: 'dashboard',
    },
  )
)
.addPanels(
  layout.generate_grid([
    panels.app.max_real_msg_sec(ds, vars) { gridPos: pos._4 },
    panels.app.max_concurrent_sockets(ds, vars) { gridPos: pos._4 },
    panels.app.active_sockets(ds, vars) { gridPos: pos._2 },

    panels.app.messages_sec_real(ds, vars) { gridPos: pos._4 },
    panels.app.messages_sec_overhead(ds, vars) { gridPos: pos._4 },
    panels.app.total_sockets(ds, vars) { gridPos: pos._2 },

    panels.app.app_cpu_memory('eu', ds, vars) { gridPos: pos._3 },
    panels.app.app_cpu_memory('us', ds, vars) { gridPos: pos._3 },
    panels.app.app_cpu_memory('ap', ds, vars) { gridPos: pos._3 },

    panels.app.healthy_hosts('eu', ds, vars) { gridPos: pos._6 },
    panels.app.healthy_hosts('us', ds, vars) { gridPos: pos._6 },
    panels.app.healthy_hosts('ap', ds, vars) { gridPos: pos._6 },
    panels.app.active_nlb_flows('eu', ds, vars) { gridPos: pos._6 },
    panels.app.active_nlb_flows('us', ds, vars) { gridPos: pos._6 },
    panels.app.active_nlb_flows('ap', ds, vars) { gridPos: pos._6 },

    panels.app.sockets_target('eu', ds, vars) { gridPos: pos._6 },
    panels.app.sockets_target('us', ds, vars) { gridPos: pos._6 },
    panels.app.sockets_target('ap', ds, vars) { gridPos: pos._6 },
    panels.app.nlb_target_resets('eu', ds, vars) { gridPos: pos._6 },
    panels.app.nlb_target_resets('us', ds, vars) { gridPos: pos._6 },
    panels.app.nlb_target_resets('ap', ds, vars) { gridPos: pos._6 },

    panels.app.mailbox_messages(ds, vars) { gridPos: pos._2 },
    panels.app.project_cache_hit_ratio(ds, vars) { gridPos: pos._2 },

    panels.app.persistent_mailbox_latency(ds, vars) { gridPos: pos._2 },
    panels.app.ephemeral_mailbox_latency(ds, vars) { gridPos: pos._2 },

    panels.app.unicast_status_code(ds, vars) { gridPos: pos._2 },
    panels.app.load_test_connected_clients(ds, vars) { gridPos: pos._2 },
    panels.app.delivery_broadcast(ds, vars) { gridPos: pos._2 },
    panels.app.rpc_task_duration(ds, vars) { gridPos: pos._2 },
    panels.app.rpc_task_duration_by_task(ds, vars) { gridPos: pos._2 },
    panels.app.rpc_task_count(ds, vars) { gridPos: pos._2 },
    panels.app.gossip_triggers(ds, vars) { gridPos: pos._2 },
    panels.app.delivery_failed(ds, vars) { gridPos: pos._2 },
    panels.app.delivery_success(ds, vars) { gridPos: pos._2 },

    panels.app.unicast_errors(ds, vars) { gridPos: pos._2 },
    panels.app.load_test_latency(ds, vars) { gridPos: pos._2 },
    panels.app.jsonrpc_errors(ds, vars) { gridPos: pos._2 },

    ////////////////////////////////////////////////////////////////////////////
    grafana.panels.text(
      content='# Canary (default)',
      transparent=true
    ) { gridPos: pos.title },

    panels.canary.canary('root', 'default', ds, vars) { gridPos: pos._4 },
    panels.canary.canary('eu', 'default', ds, vars) { gridPos: pos._4 },
    panels.canary.canary('us', 'default', ds, vars) { gridPos: pos._4 },
    panels.canary.canary('ap', 'default', ds, vars) { gridPos: pos._4 },

    panels.canary.handshake_latency('default', ds, vars) { gridPos: pos._5 },
    panels.canary.latency('default', ds, vars) { gridPos: pos._5 },
    panels.canary.ping_latency('default', ds, vars) { gridPos: pos._5 },
    panels.canary.propose_pairing_latency('default', ds, vars) { gridPos: pos._5 },
    panels.canary.settle_pairing_latency('default', ds, vars) { gridPos: pos._5 },

    panels.canary.auth_canary(ds, vars) { gridPos: pos._2 },
    panels.canary.chat_canary(ds, vars) { gridPos: pos._2 },

    panels.canary.auth_canary_latency(ds, vars) { gridPos: pos._2 },
    panels.canary.chat_canary_latency(ds, vars) { gridPos: pos._2 },

    ////////////////////////////////////////////////////////////////////////////
    grafana.panels.text(
      content='# Canary (irn)',
      transparent=true
    ) { gridPos: pos.title },

    panels.canary.canary('root', 'irn', ds, vars) { gridPos: pos._4 },
    panels.canary.canary('eu', 'irn', ds, vars) { gridPos: pos._4 },
    panels.canary.canary('us', 'irn', ds, vars) { gridPos: pos._4 },
    panels.canary.canary('ap', 'irn', ds, vars) { gridPos: pos._4 },

    panels.canary.handshake_latency('irn', ds, vars) { gridPos: pos._5 },
    panels.canary.latency('irn', ds, vars) { gridPos: pos._5 },
    panels.canary.ping_latency('irn', ds, vars) { gridPos: pos._5 },
    panels.canary.propose_pairing_latency('irn', ds, vars) { gridPos: pos._5 },
    panels.canary.settle_pairing_latency('irn', ds, vars) { gridPos: pos._5 },

    ////////////////////////////////////////////////////////////////////////////
    grafana.panels.text(
      content='# WebHooks',
      transparent=true
    ) { gridPos: pos.title },

    panels.webhooks.in_flight_messages(ds, vars) { gridPos: pos._2 },
    panels.webhooks.dispatch(ds, vars) { gridPos: pos._2 },

    panels.webhooks.available_messages(ds, vars) { gridPos: pos._2 },
    panels.webhooks.queue(ds, vars) { gridPos: pos._2 },

    panels.webhooks.cpu(ds, vars) { gridPos: pos._3 },
    panels.webhooks.memory(ds, vars) { gridPos: pos._3 },
    panels.webhooks.storage(ds, vars) { gridPos: pos._3 },
  ])
)
