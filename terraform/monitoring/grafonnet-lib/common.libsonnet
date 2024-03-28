{
  legend(
    asTable = null,
    calcs = [],
    displayMode = $.legendDisplayModes.list,
    isVisible = null,
    placement = $.legendPlacements.bottom,
    showLegend = null,
    sortBys = null,
    sortDescs = null,
    width = null,
  ):: self + {
    [if asTable != null then 'asTable']: asTable,
    calcs: calcs,
    displayMode: displayMode,
    [if isVisible != null then 'isVisible']: isVisible,
    placement: placement,
    [if showLegend != null then 'showLegend']: showLegend,
    [if sortBys != null then 'sortBys']: sortBys,
    [if sortDescs != null then 'sortDescs']: sortDescs,
    [if width != null then 'width']: width,
  },

  legendDisplayModes:: {
    hidden::  'hidden',
    list::    'list',
    table::   'table',
  },

  legendPlacements:: {
    bottom: 'bottom',
    right:  'right',
  }
} + {
  tooltip(
    mode = $.tooltipModes.multi,
    sort = $.sortOrders.none,
  ):: self + {
    [if mode != null then 'mode']: mode,
    [if sort != null then 'sort']: sort,
  },

  tooltipModes:: {
    multi:      'multi',
    none:       'none',
    single:     'single',
  },

  sortOrders:: {
    ascending:  'asc',
    descending: 'desc',
    none:       'none',
  },
}
