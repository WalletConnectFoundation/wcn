local grafana         = import '../grafana.libsonnet';
local strings         = import '../utils/strings.libsonnet';

local colors = {
  cpu:      'blue',
  memory:   'purple',

  ok:       'green',
  warn:     'orange', // '#EAB839',
  critical: 'red',
};

{
  timeseries:: grafana.panels.timeseries().createConfiguration(
    scaleDistribution = {
      type : 'linear'
    },
    legend  = grafana.common.legend(),
    tooltip = grafana.common.tooltip(),
  ),

  timeseries_resource:: grafana.panels.timeseries().createConfiguration(
    thresholdsStyle = {
      mode: 'area',
    },
    scaleDistribution = {
      type: 'linear'
    },
    legend  = grafana.common.legend(),
    tooltip = grafana.common.tooltip(),
  )
  .addThreshold({
    color : colors.warn, // '#EAB839',
    value : 40
  })
  .addThreshold({
    color : colors.critical,
    value : 70
  })
  .withUnit('percent')
  .withSoftLimit(
  axisSoftMin = 0,
  axisSoftMax = 40,
  ),
}
