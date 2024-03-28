{
  target(
    expr,
    hide            = null,
    refId           = null,
  ):: {
    type: 'math',
    datasource : {
      type: "__expr__",
      uid:  "__expr__"
    },
    expression: expr,

    [if hide != null            then 'hide']: hide,
    [if refId != null           then 'refId']: refId,
  },
}
