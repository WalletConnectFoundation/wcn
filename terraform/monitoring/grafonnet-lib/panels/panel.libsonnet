{
  /**
   * Dashboard panel.
   *
   * @name timeseries_panel.new
   *
   * @param title The title of the singlestat panel.


   * @method addTarget(target) Adds a target object.
   * @method addTargets(targets) Adds an array of targets.
   * @method addLink(link) Adds a [panel link](https://grafana.com/docs/grafana/latest/linking/panel-links/).
   * @method addLinks(links) Adds an array of links.
   */
  new(
    title           = '',
    description     = null,
    datasource      = null,
    fieldConfig       = null,
    interval        = null,
    libraryPanel    = null,
    maxDataPoints   = null,
    pluginVersion   = '8.4.7',
    repeat          = null,
    repeatDirection = null,
    repeatPanelId   = null,
    tags            = null,
    timeFrom        = null,
    timeRegions     = null,
    timeShift       = null,
    transformations = null,
    transparent     = null,
  )::
    self {
      title: title,
      [if description != '' then 'description']: description,
      [if datasource != null then 'datasource']: datasource,
      [if fieldConfig != null then 'fieldConfig']: fieldConfig,
      [if interval != null then 'interval']: interval,
      [if libraryPanel != null then 'libraryPanel']: libraryPanel,
      [if maxDataPoints != null then 'maxDataPoints']: maxDataPoints,
      options+: {},
      pluginVersion: pluginVersion,
      [if repeat != null then 'repeat']: repeat,
      [if repeatDirection != null then 'repeatDirection']: repeatDirection,
      [if repeatPanelId != null then 'repeatPanelId']: repeatPanelId,
      [if tags != null then 'tags']: tags,
      [if timeFrom != null then 'timeFrom']: timeFrom,
      [if timeRegions != null then 'timeRegions']: timeRegions,
      [if timeShift != null then 'timeShift']: timeShift,
      [if transparent != null then 'transparent']: transparent,
    } +

    {
      // Targets
      targets: [],
      _nextTarget:: 0,
      addTarget(target):: self {
        local nextTarget = super._nextTarget,
        _nextTarget: nextTarget + 1,
        targets+: [
          if std.objectHas(target, 'refId') && target.refId != null then
            target
          else
            target { refId: std.char(std.codepoint('A') + nextTarget) }
        ],
      },
      addTargets(targets):: std.foldl(function(p, t) p.addTarget(t), targets, self),
    } +

    {
      // Links
      links: [],
      addLink(link):: self {
        links+: [link],
      },
      addLinks(links):: std.foldl(function(p, l) p.addLink(l), links, self),
    } +

    {
      // Alerts
      setAlert(alert):: self {
        alert: alert
      },
    },
}
