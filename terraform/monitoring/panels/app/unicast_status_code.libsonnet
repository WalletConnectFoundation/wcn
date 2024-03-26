local grafana = import '../../grafonnet-lib/grafana.libsonnet';
local panels = grafana.panels;
local targets = grafana.targets;

local defaults = import '../defaults.libsonnet';

{
  new(ds, vars)::
    panels.timeseries(
      title='Unicast by Status Code',
      datasource=ds.prometheus,
    )
    .configure(defaults.configuration.timeseries_tr80)

    .addTarget(targets.prometheus(
      datasource=ds.prometheus,
      expr='sum by (status_code)(rate(http_requests_finished_total{aws_ecs_task_family="%s_eu-central-1_relay",route_name="unicast"}[5m]))' % vars.environment,
      refId='eu_central_1_sum',
      exemplar=true,
      hide=true,
    ))
    .addTarget(targets.prometheus(
      datasource=ds.prometheus,
      expr='sum by (status_code)(rate(http_requests_finished_total{aws_ecs_task_family="%s_us-east-1_relay",route_name="unicast"}[5m]))' % vars.environment,
      refId='us_east_1_sum',
      exemplar=true,
      hide=true,
    ))
    .addTarget(targets.prometheus(
      datasource=ds.prometheus,
      expr='sum by (status_code)(rate(http_requests_finished_total{aws_ecs_task_family="%s_ap-southeast-1_relay",route_name="unicast"}[5m]))' % vars.environment,
      refId='ap_southeast_1_sum',
      exemplar=true,
      hide=true,
    ))
    .addTarget(targets.math(
      expr='$eu_central_1_sum+$us_east_1_sum+$ap_southeast_1_sum',
      refId='XRegion',
    )),
}
