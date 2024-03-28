local grafana         = import '../grafana.libsonnet';
local strings         = import '../utils/strings.libsonnet';

local panels = grafana.panels;

local defaults = {
  priority:   null,
  limit:      70,
  timeStart:  '5m',
  period:     '5m',
  frequency:  '1m',
  refid:      'CPU',
  reducer:    'Avg',
};

{
  memory(
    namespace,
    env,
    title         = 'Memory Utilization',
    notifications = [],
    priority      = null,
    limit         = defaults.limit,
    timeStart     = defaults.timeStart,
    period        = defaults.period,
    frequency     = defaults.frequency,
    reducer       = defaults.reducer,
    refid         = 'Mem',
  ):: grafana.alert.new(
    namespace     = namespace,
    name          = "%s - %s - Memory Utilization alert" % [strings.capitalize(env), title],
    message       = '%s - %s - Memory utilization is high (over %d)' % [strings.capitalize(env), title, limit],
    period        = period,
    frequency     = frequency,
    notifications = notifications,
    alertRuleTags = [if priority != null then { og_priority: priority } else {}],
    conditions    = [ $.conditions.memory(limit, timeStart, reducer, refid) ],
  ),

  cpu(
    namespace,
    env,
    title         = 'CPU Utilization',
    notifications = [],
    priority      = null,
    limit         = defaults.limit,
    timeStart     = defaults.timeStart,
    period        = defaults.period,
    frequency     = defaults.frequency,
    reducer       = defaults.reducer,
    refid         = 'CPU',
  ):: grafana.alert.new(
    namespace     = namespace,
    name          = "%s - %s - CPU Utilization alert" % [strings.capitalize(env), title],
    message       = '%s - %s - CPU utilization is high (over %d)' % [strings.capitalize(env), title, limit],
    period        = period,
    frequency     = frequency,
    notifications = notifications,
    alertRuleTags = [if priority != null then { og_priority: priority } else {}],
    conditions    = [ $.conditions.cpu(limit, timeStart, reducer, refid) ],
  ),

  cpu_mem(
    namespace,
    env,
    title         = 'CPU/Memory Utilization',
    notifications = [],
    priority      = null,
    period        = defaults.period,
    frequency     = defaults.frequency,

    cpu           = {
      limit         : defaults.limit,
      timeStart     : defaults.timeStart,
      reducer       : defaults.reducer,
      refid         : 'CPU',
    },
    memory        = {
      limit         : defaults.limit,
      timeStart     : defaults.timeStart,
      reducer       : defaults.reducer,
      refid         : 'Mem',
    },
  ):: grafana.alert.new(
    namespace     = namespace,
    name          = "%s - %s - CPU/Memory Utilization alert" % [strings.capitalize(env), title],
    message       = '%s - %s - CPU/Memory utilization is high (over %d/%d)' % [strings.capitalize(env), title, cpu.limit, memory.limit],
    period        = period,
    frequency     = frequency,
    notifications = notifications,
    alertRuleTags = [if priority != null then { og_priority: priority } else {}],
    conditions    = [
      $.conditions.cpu(cpu.limit, cpu.timeStart, cpu.reducer, cpu.refid),
      $.conditions.memory(memory.limit, memory.timeStart, memory.reducer, memory.refid),
    ],
  ),

  conditions:: {
    cpu(
      limit         = defaults.limit,
      timeStart     = defaults.timeStart,
      reducer       = defaults.reducer,
      refid         = 'CPU',
    ):: grafana.alertCondition.new(
      evaluatorParams = [ limit ],
      evaluatorType   = 'gt',
      operatorType    = 'and',
      queryRefId      = '%s_%s' % [refid, reducer],
      queryTimeStart  = timeStart,
      reducerType     = std.asciiLower(reducer),
    ),

    memory(
      limit         = defaults.limit,
      timeStart     = '5m',
      reducer       = 'Avg',
      refid         = 'Mem',
    ):: grafana.alertCondition.new(
      evaluatorParams = [ limit ],
      evaluatorType   = 'gt',
      operatorType    = 'and',
      queryRefId      = '%s_%s' % [refid, reducer],
      queryTimeStart  = timeStart,
      reducerType     = std.asciiLower(reducer),
    ),
  },
}
