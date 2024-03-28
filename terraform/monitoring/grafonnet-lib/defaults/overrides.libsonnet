local grafana         = import '../grafana.libsonnet';

local defaults = {
  refidCPU    : 'CPU',
  refidMemory : 'Mem'
};

local colors = {
  cpu:      'blue',
  memory:   'purple',

  ok:       'green',
  warn:     'orange', // '#EAB839',
  critical: 'red',
};

{
  cpu(cfg, refid=defaults.refidCPU):: cfg
    .addOverride(grafana.override.new(
      name = '%s_Max' % refid,
      properties = [{
        id: 'color',
        value: {
          mode:       'fixed',
          fixedColor: 'dark-' + colors.cpu
        }
      }],
    ))
    .addOverride(grafana.override.new(
      name = '%s_Avg' % refid,
      properties = [{
        id: 'color',
        value: {
          mode:       'fixed',
          fixedColor: colors.cpu
        }
      }],
    )),

  memory(cfg, refid=defaults.refidMemory):: cfg
    .addOverride(grafana.override.new(
      name = '%s_Max' % refid,
      properties = [{
        id: 'color',
        value: {
          mode:       'fixed',
          fixedColor: 'dark-' + colors.memory
        }
      }],
    ))
    .addOverride(grafana.override.new(
      name = '%s_Avg' % refid,
      properties = [{
        id: 'color',
        value: {
          mode:       'fixed',
          fixedColor: colors.memory
        }
      }],
    )),

    cpu_memory(cfg, refidCPU=defaults.refidCPU, refidMemory=defaults.refidMemory):: $.memory($.cpu(cfg, refidCPU), refidMemory),
}
