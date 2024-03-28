{
  /**
   * Creates an [annotation](https://grafana.com/docs/grafana/latest/alerting/fundamentals/annotation-label/)
   *
   *
   * @name annotation.new
   *
   * @param name (default: 'Annotations & Alerts')  Name of annotation.
   * @param datasource (default: '-- Grafana --')       Datasource to use for annotation.
   * @param expr (optional)
   * @param target (optional)
   * @param enable (default: true)                      Whether annotation is enabled.
   * @param hide (default: true)                        Whether to hide the annotation.
   * @param iconColor (default: 'rgba(0, 211, 255, 1)') Annotation icon color.
   * @param tags (default: null)
   * @param type (default: 'dashboard')
   * @param builtIn (default: 1)
   */
 new(
    name        = 'Annotations & Alerts',
    datasource  = '-- Grafana --',
    expr        = null,
    target      = null,
    enable      = true,
    hide        = true,
    iconColor   = 'rgba(0, 211, 255, 1)',
    tags        = null,
    type        = 'dashboard',
    builtIn     = 1,
 ):: {
  datasource: datasource,
  enable: enable,
  [if expr != null then 'expr']: expr,
  [if target != null then 'target']: target,
  hide: hide,
  iconColor: iconColor,
  name: name,
  showIn: 0,
  [if tags != null then 'tags']: tags,
  type: type,
  [if builtIn != null then 'builtIn']: builtIn,
 },
}
