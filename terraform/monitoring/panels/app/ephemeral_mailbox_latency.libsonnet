local grafana = import '../../grafonnet-lib/grafana.libsonnet';
local panels = grafana.panels;
local targets = grafana.targets;
local alert = grafana.alert;
local alertCondition = grafana.alertCondition;

local defaults = import '../defaults.libsonnet';

local alert_condition(metric) =
  alertCondition.new(
    evaluatorParams=[1],
    evaluatorType='lt',
    operatorType='or',
    queryRefId=metric,
    queryTimeStart='5m',
    queryTimeEnd='now',
    reducerType='sum',
  );

local ephemeral_mailbox_alert(vars) = alert.new(
  namespace='Relay',
  name='Relay %s - Ephemeral Mailbox Latency No Data alert' % [vars.environment],
  message='Relay %s - Ephemeral Mailbox Latency No Data' % [vars.environment],
  period='5m',
  frequency='1m',
  noDataState='alerting',
  notifications=vars.notifications,
  alertRuleTags={
    og_priority: 'P3',
  },
  conditions=std.map(alert_condition, [
    'store_eu_central_1_count',
    'store_us_east_1_count',
    'store_ap_southeast_1_count',
    'remove_us_east_1_count',
    'remove_eu_central_1_count',
    'remove_ap_southeast_1_count',
  ])
);

local _configuration = defaults.configuration.timeseries_tr80
                       .withUnit('ms');

{
  new(ds, vars)::
    panels.timeseries(
      title='Ephemeral Mailbox Latency',
      datasource=ds.prometheus,
    )
    .configure(_configuration)
    .setAlert(ephemeral_mailbox_alert(vars))
    .addTarget(targets.prometheus(
      datasource=ds.prometheus,
      expr='sum(rate(mailbox_message_store_time_sum{aws_ecs_task_family="%s_eu-central-1_relay",mailbox_kind="ephemeral"}[5m]))' % vars.environment,
      refId='store_eu_central_1_sum',
      exemplar=true,
      hide=true,
    ))
    .addTarget(targets.prometheus(
      datasource=ds.prometheus,
      expr='sum(rate(mailbox_message_store_time_count{aws_ecs_task_family="%s_eu-central-1_relay",mailbox_kind="ephemeral"}[5m]))' % vars.environment,
      refId='store_eu_central_1_count',
      exemplar=true,
      hide=true,
    ))
    .addTarget(targets.math(
      expr='$store_eu_central_1_sum/$store_eu_central_1_count',
      refId='store (eu-central-1)',
    ))

    .addTarget(targets.prometheus(
      datasource=ds.prometheus,
      expr='sum(rate(mailbox_message_store_time_sum{aws_ecs_task_family="%s_us-east-1_relay",mailbox_kind="ephemeral"}[5m]))' % vars.environment,
      refId='store_us_east_1_sum',
      exemplar=true,
      hide=true,
    ))
    .addTarget(targets.prometheus(
      datasource=ds.prometheus,
      expr='sum(rate(mailbox_message_store_time_count{aws_ecs_task_family="%s_us-east-1_relay",mailbox_kind="ephemeral"}[5m]))' % vars.environment,
      refId='store_us_east_1_count',
      exemplar=true,
      hide=true,
    ))
    .addTarget(targets.math(
      expr='$store_us_east_1_sum/$store_us_east_1_count',
      refId='store (us-east-1)',
    ))

    .addTarget(targets.prometheus(
      datasource=ds.prometheus,
      expr='sum(rate(mailbox_message_store_time_sum{aws_ecs_task_family="%s_ap-southeast-1_relay",mailbox_kind="ephemeral"}[5m]))' % vars.environment,
      refId='store_ap_southeast_1_sum',
      exemplar=true,
      hide=true,
    ))
    .addTarget(targets.prometheus(
      datasource=ds.prometheus,
      expr='sum(rate(mailbox_message_store_time_count{aws_ecs_task_family="%s_ap-southeast-1_relay",mailbox_kind="ephemeral"}[5m]))' % vars.environment,
      refId='store_ap_southeast_1_count',
      exemplar=true,
      hide=true,
    ))
    .addTarget(targets.math(
      expr='$store_ap_southeast_1_sum/$store_ap_southeast_1_count',
      refId='store (ap-southeast-1)',
    ))

    .addTarget(targets.prometheus(
      datasource=ds.prometheus,
      expr='sum(rate(mailbox_message_retrieve_time_sum{aws_ecs_task_family="%s_eu-central-1_relay",mailbox_kind="ephemeral"}[5m]))' % vars.environment,
      refId='retrieve_eu_central_1_sum',
      exemplar=true,
      hide=true,
    ))
    .addTarget(targets.prometheus(
      datasource=ds.prometheus,
      expr='sum(rate(mailbox_message_retrieve_time_count{aws_ecs_task_family="%s_eu-central-1_relay",mailbox_kind="ephemeral"}[5m]))' % vars.environment,
      refId='retrieve_eu_central_1_count',
      exemplar=true,
      hide=true,
    ))
    .addTarget(targets.math(
      expr='$retrieve_eu_central_1_sum/$retrieve_eu_central_1_count',
      refId='retrieve (eu-central-1)',
    ))

    .addTarget(targets.prometheus(
      datasource=ds.prometheus,
      expr='sum(rate(mailbox_message_retrieve_time_sum{aws_ecs_task_family="%s_us-east-1_relay",mailbox_kind="ephemeral"}[5m]))' % vars.environment,
      refId='retrieve_us_east_1_sum',
      exemplar=true,
      hide=true,
    ))
    .addTarget(targets.prometheus(
      datasource=ds.prometheus,
      expr='sum(rate(mailbox_message_retrieve_time_count{aws_ecs_task_family="%s_us-east-1_relay",mailbox_kind="ephemeral"}[5m]))' % vars.environment,
      refId='retrieve_us_east_1_count',
      exemplar=true,
      hide=true,
    ))
    .addTarget(targets.math(
      expr='$retrieve_us_east_1_sum/$retrieve_us_east_1_count',
      refId='retrieve (us-east-1)',
    ))

    .addTarget(targets.prometheus(
      datasource=ds.prometheus,
      expr='sum(rate(mailbox_message_retrieve_time_sum{aws_ecs_task_family="%s_ap-southeast-1_relay",mailbox_kind="ephemeral"}[5m]))' % vars.environment,
      refId='retrieve_ap_southeast_1_sum',
      exemplar=true,
      hide=true,
    ))
    .addTarget(targets.prometheus(
      datasource=ds.prometheus,
      expr='sum(rate(mailbox_message_retrieve_time_count{aws_ecs_task_family="%s_ap-southeast-1_relay",mailbox_kind="ephemeral"}[5m]))' % vars.environment,
      refId='retrieve_ap_southeast_1_count',
      exemplar=true,
      hide=true,
    ))
    .addTarget(targets.math(
      expr='$retrieve_ap_southeast_1_sum/$retrieve_ap_southeast_1_count',
      refId='retrieve (ap-southeast-1)',
    ))

    .addTarget(targets.prometheus(
      datasource=ds.prometheus,
      expr='sum(rate(mailbox_message_remove_time_sum{aws_ecs_task_family="%s_eu-central-1_relay",mailbox_kind="ephemeral"}[5m]))' % vars.environment,
      refId='remove_eu_central_1_sum',
      exemplar=true,
      hide=true,
    ))
    .addTarget(targets.prometheus(
      datasource=ds.prometheus,
      expr='sum(rate(mailbox_message_remove_time_count{aws_ecs_task_family="%s_eu-central-1_relay",mailbox_kind="ephemeral"}[5m]))' % vars.environment,
      refId='remove_eu_central_1_count',
      exemplar=true,
      hide=true,
    ))
    .addTarget(targets.math(
      expr='$remove_eu_central_1_sum/$remove_eu_central_1_count',
      refId='remove (eu-central-1)',
    ))

    .addTarget(targets.prometheus(
      datasource=ds.prometheus,
      expr='sum(rate(mailbox_message_remove_time_sum{aws_ecs_task_family="%s_us-east-1_relay",mailbox_kind="ephemeral"}[5m]))' % vars.environment,
      refId='remove_us_east_1_sum',
      exemplar=true,
      hide=true,
    ))
    .addTarget(targets.prometheus(
      datasource=ds.prometheus,
      expr='sum(rate(mailbox_message_remove_time_count{aws_ecs_task_family="%s_us-east-1_relay",mailbox_kind="ephemeral"}[5m]))' % vars.environment,
      refId='remove_us_east_1_count',
      exemplar=true,
      hide=true,
    ))
    .addTarget(targets.math(
      expr='$remove_us_east_1_sum/$remove_us_east_1_count',
      refId='remove (us-east-1)',
    ))

    .addTarget(targets.prometheus(
      datasource=ds.prometheus,
      expr='sum(rate(mailbox_message_remove_time_sum{aws_ecs_task_family="%s_ap-southeast-1_relay",mailbox_kind="ephemeral"}[5m]))' % vars.environment,
      refId='remove_ap_southeast_1_sum',
      exemplar=true,
      hide=true,
    ))
    .addTarget(targets.prometheus(
      datasource=ds.prometheus,
      expr='sum(rate(mailbox_message_remove_time_count{aws_ecs_task_family="%s_ap-southeast-1_relay",mailbox_kind="ephemeral"}[5m]))' % vars.environment,
      refId='remove_ap_southeast_1_count',
      exemplar=true,
      hide=true,
    ))
    .addTarget(targets.math(
      expr='$remove_ap_southeast_1_sum/$remove_ap_southeast_1_count',
      refId='remove (ap-southeast-1)',
    )),
}
