{
  /**
   * Creates a [CloudWatch target](https://grafana.com/docs/grafana/latest/datasources/cloudwatch/)
   *
   * @name cloudwatch.target
   *
   * @return Panel target
   */

  target(
    id                = null,
    alias             = null,
    datasource        = null,
    dimensions        = {},
    expression        = '',
    matchExact        = false,
    metricEditorMode  = $.metricEditorModes.builder,
    metricName        = null,
    metricQueryType   = $.metricQueryTypes.search,
    namespace,
    period            = 'auto',
    queryMode         = $.cloudWatchQueryModes.metrics,
    region            = 'default',
    sql               = null,
    sqlExpression     = null,
    statistic         = $.statisticModes.average,
    hide              = null,
    refId             = null,
  ):: {
    [if id            != null then 'id']: id,
    [if hide          != null then 'hide']: hide,
    [if alias         != null then 'alias']: alias,
    [if datasource    != null then 'datasource']: datasource,
    dimensions:       dimensions,
    expression:       expression,
    matchExact:       matchExact,
    metricEditorMode: metricEditorMode,
    metricName:       metricName,
    metricQueryType:  metricQueryType,
    namespace:        namespace,
    period:           period,
    queryMode:        queryMode,
    region:           region,
    [if sql           != null then 'sql']: sql,
    [if sqlExpression != null then 'sqlExpression']: sqlExpression,
    statistic:        statistic,
    [if refId         != null then 'refId']: refId,
  },

  metricEditorModes:: {
    builder:  0,
    code:     1,
  },

  metricQueryTypes:: {
    search:  0,
    query:   1,
  },

  cloudWatchQueryModes:: {
    metrics:      'Metrics',
    logs:         'Logs',
    annotations:  'Annotations',
  },

  statisticModes: {
    average: 'Average',
    maximum: 'Maximum',
    minimum: 'Minimum',
  },
}
