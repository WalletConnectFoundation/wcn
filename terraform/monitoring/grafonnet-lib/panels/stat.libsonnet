local panel = import 'panel.libsonnet';
local fieldConfig = import 'field_config.libsonnet';

  /**
   * Stat panel.
   *
   */
{
  type:       'stat',
  fieldConfig:  fieldConfig.new(),
  options: {
    textMode:     $.textModes.auto,
    colorMode:    $.colorModes.value,
    graphMode:    $.graphModes.none,
    justifyMode:  $.justifyModes.auto,
    orientation:  $.orientations.auto,
  },

  setOptions(
    textMode      = null,
    colorMode     = null,
    graphMode     = null,
    justifyMode   = null,
    orientation   = null,
    reduceOptions = null,
  ):: self {
    options+: {
      [if textMode != null      then 'textMode']: textMode,
      [if colorMode != null     then 'colorMode']: colorMode,
      [if graphMode != null     then 'graphMode']: graphMode,
      [if justifyMode != null   then 'justifyMode']: justifyMode,
      [if orientation != null   then 'orientation']: orientation,
      [if reduceOptions != null then 'reduceOptions']: reduceOptions,
    },
  },

  textModes:: {
    auto:         'auto',
    value:        'value',
    valueAndName: 'value_and_name',
    name:         'name',
    none:         'none',
  },

  colorModes:: {
    value:        'value',
    background:   'background',
    none:         'none',
  },

  graphModes:: {
    area:         'area',
    line:         'line',
    none:         'none',
  },

  justifyModes:: {
    auto:         'auto',
    center:       'center',
  },

  orientations:: {
    auto:         'auto',
    horizontal:   'horizontal',
    vertical:     'vertical',
  },
} + panel
