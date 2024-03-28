{
  /**
   * Returns a new condition of alert of graph panel.
   * Currently the only condition type that exists is a Query condition
   * that allows to specify a query letter, time range and an aggregation function.
   *
   * @name alertCondition.new
   *
   * @param evaluatorParams Value of threshold
   * @param evaluatorType Type of threshold
   * @param operatorType Operator between conditions
   * @param queryRefId The letter defines what query to execute from the Metrics tab
   * @param queryTimeStart Begging of time range
   * @param queryTimeEnd End of time range
   * @param reducerParams Params of an aggregation function
   * @param reducerType Name of an aggregation function
   *
   * @return A json that represents a condition of alert
   */
  new(
    namespace,
    name,
    alertRuleTags       = {},
    conditions          = [],
    executionErrorState = "alerting",
    period              = "5m",
    frequency           = "1m",
    handler             = 1,
    message,
    noDataState         = "alerting",
    notifications        = []

  ):: {
    name:                 "%s - %s" % [namespace, name],
    message:              "%s - %s" % [namespace, message],
    alertRuleTags:        alertRuleTags,
    conditions:           conditions,
    executionErrorState:  executionErrorState,
    'for':                period,
    frequency:            frequency,
    handler:              handler,
    noDataState:          noDataState,
    notifications:        notifications,
  },

  withCondition(condition):: self + {
    conditions+: [condition]
  },
  withConditions(conditions):: self + {
    conditions+: conditions
  },
}
