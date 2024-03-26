local grafana = import '../../grafonnet-lib/grafana.libsonnet';
local panels = grafana.panels;
local targets = grafana.targets;

local defaults = import '../defaults.libsonnet';

local _configuration = defaults.configuration.timeseries_tr80
                       .withUnit('ms');

{
  new(ds, vars)::
    panels.timeseries(
      title='Persistent Mailbox Latency',
      datasource=ds.prometheus,
    )
    .configure(_configuration)

    .addTarget(targets.prometheus(
      datasource=ds.prometheus,
      expr='sum(rate(mailbox_message_store_time_sum{aws_ecs_task_family="%s_eu-central-1_relay",mailbox_kind="persistent"}[5m]))' % vars.environment,
      refId='store_eu_central_1_sum',
      exemplar=true,
      hide=true,
    ))
    .addTarget(targets.prometheus(
      datasource=ds.prometheus,
      expr='sum(rate(mailbox_message_store_time_count{aws_ecs_task_family="%s_eu-central-1_relay",mailbox_kind="persistent"}[5m]))' % vars.environment,
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
      expr='sum(rate(mailbox_message_store_time_sum{aws_ecs_task_family="%s_us-east-1_relay",mailbox_kind="persistent"}[5m]))' % vars.environment,
      refId='store_us_east_1_sum',
      exemplar=true,
      hide=true,
    ))
    .addTarget(targets.prometheus(
      datasource=ds.prometheus,
      expr='sum(rate(mailbox_message_store_time_count{aws_ecs_task_family="%s_us-east-1_relay",mailbox_kind="persistent"}[5m]))' % vars.environment,
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
      expr='sum(rate(mailbox_message_store_time_sum{aws_ecs_task_family="%s_ap-southeast-1_relay",mailbox_kind="persistent"}[5m]))' % vars.environment,
      refId='store_ap_southeast_1_sum',
      exemplar=true,
      hide=true,
    ))
    .addTarget(targets.prometheus(
      datasource=ds.prometheus,
      expr='sum(rate(mailbox_message_store_time_count{aws_ecs_task_family="%s_ap-southeast-1_relay",mailbox_kind="persistent"}[5m]))' % vars.environment,
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
      expr='sum(rate(mailbox_message_retrieve_time_sum{aws_ecs_task_family="%s_eu-central-1_relay",mailbox_kind="persistent"}[5m]))' % vars.environment,
      refId='retrieve_eu_central_1_sum',
      exemplar=true,
      hide=true,
    ))
    .addTarget(targets.prometheus(
      datasource=ds.prometheus,
      expr='sum(rate(mailbox_message_retrieve_time_count{aws_ecs_task_family="%s_eu-central-1_relay",mailbox_kind="persistent"}[5m]))' % vars.environment,
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
      expr='sum(rate(mailbox_message_retrieve_time_sum{aws_ecs_task_family="%s_us-east-1_relay",mailbox_kind="persistent"}[5m]))' % vars.environment,
      refId='retrieve_us_east_1_sum',
      exemplar=true,
      hide=true,
    ))
    .addTarget(targets.prometheus(
      datasource=ds.prometheus,
      expr='sum(rate(mailbox_message_retrieve_time_count{aws_ecs_task_family="%s_us-east-1_relay",mailbox_kind="persistent"}[5m]))' % vars.environment,
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
      expr='sum(rate(mailbox_message_retrieve_time_sum{aws_ecs_task_family="%s_ap-southeast-1_relay",mailbox_kind="persistent"}[5m]))' % vars.environment,
      refId='retrieve_ap_southeast_1_sum',
      exemplar=true,
      hide=true,
    ))
    .addTarget(targets.prometheus(
      datasource=ds.prometheus,
      expr='sum(rate(mailbox_message_retrieve_time_count{aws_ecs_task_family="%s_ap-southeast-1_relay",mailbox_kind="persistent"}[5m]))' % vars.environment,
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
      expr='sum(rate(mailbox_message_remove_time_sum{aws_ecs_task_family="%s_eu-central-1_relay",mailbox_kind="persistent"}[5m]))' % vars.environment,
      refId='remove_eu_central_1_sum',
      exemplar=true,
      hide=true,
    ))
    .addTarget(targets.prometheus(
      datasource=ds.prometheus,
      expr='sum(rate(mailbox_message_remove_time_count{aws_ecs_task_family="%s_eu-central-1_relay",mailbox_kind="persistent"}[5m]))' % vars.environment,
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
      expr='sum(rate(mailbox_message_remove_time_sum{aws_ecs_task_family="%s_us-east-1_relay",mailbox_kind="persistent"}[5m]))' % vars.environment,
      refId='remove_us_east_1_sum',
      exemplar=true,
      hide=true,
    ))
    .addTarget(targets.prometheus(
      datasource=ds.prometheus,
      expr='sum(rate(mailbox_message_remove_time_count{aws_ecs_task_family="%s_us-east-1_relay",mailbox_kind="persistent"}[5m]))' % vars.environment,
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
      expr='sum(rate(mailbox_message_remove_time_sum{aws_ecs_task_family="%s_ap-southeast-1_relay",mailbox_kind="persistent"}[5m]))' % vars.environment,
      refId='remove_ap_southeast_1_sum',
      exemplar=true,
      hide=true,
    ))
    .addTarget(targets.prometheus(
      datasource=ds.prometheus,
      expr='sum(rate(mailbox_message_remove_time_count{aws_ecs_task_family="%s_ap-southeast-1_relay",mailbox_kind="persistent"}[5m]))' % vars.environment,
      refId='remove_ap_southeast_1_count',
      exemplar=true,
      hide=true,
    ))
    .addTarget(targets.math(
      expr='$remove_ap_southeast_1_sum/$remove_ap_southeast_1_count',
      refId='remove (ap-southeast-1)',
    )),
}
