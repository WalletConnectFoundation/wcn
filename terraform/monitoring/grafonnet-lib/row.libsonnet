local layout = import 'layout.libsonnet';

{
  /**
   * Creates a row.
   * Rows are logical dividers within a dashboard and used to group panels together.
   *
   * @name row.new
   *
   * @param title The title of the row.
   * @param collapsed (default `false`) The initial state of the row when opening the dashboard. Panels in a collapsed row are not load until the row is expanded.
   *
   * @method addPanels(panels) Appends an array of nested panels
   * @method addPanel(panel,gridPos) Appends a nested panel, with an optional grid position in grid coordinates, e.g. `gridPos={'x':0, 'y':0, 'w':12, 'h': 9}`
   */
  new(
    title='Dashboard Row',
    collapsed=false,
    gridPos=null,
  ):: {
    collapsed: collapsed,
    datasource: {
      type: 'datasource',
      uid:  'grafana'
    },
    gridPos: if gridPos != null then gridPos else { w: layout.width._1, h: 1 },
    panels:: [],
    targets: [{
      datasource: {
        type: 'datasource',
        uid:  'grafana'
      },
    }],
    title: title,
    type: "row",

    addPanels(panels):: self {
      panels+: panels,
    },
    addPanel(panel, gridPos=null):: self {
      panels+: [panel { gridPos: gridPos }],
    },
  },
}
