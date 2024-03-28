local grid_width = 24;

{
  width:: {
    _1::      grid_width,
    _2::      grid_width / 2,
    _3::      grid_width / 3,
    _4::      grid_width / 4,
    _5::      grid_width / 5,
    _6::      grid_width / 6,
    _8::      grid_width / 8,

    full::    $.width._1,
    half::    $.width._2,
    third::   $.width._3,
    quarter:: $.width._4,
    fifth::   $.width._5,
    sixth::   $.width._6,
    heighth:: $.width._8,

    // TODO: Legacy items, remove in the future
    one_third::       grid_width / 3,
    two_thirds::      grid_width / 3 * 2,
    one_fifth::       grid_width / 5,
    one_sixth::       grid_width / 6,
    three_quarters::  grid_width / 4 * 3,
  },

  /**
   * @name layout.pos
   *
   * Generate standard positions based on the specified height.
   *
   * @param height The standard height for a panel.
   *
   * @return Standard layout position values.
   */
  pos(height):: {
    _1::              { w: $.width._1,      h: height     },
    _2::              { w: $.width._2,      h: height     },
    _3::              { w: $.width._3,      h: height     },
    _4::              { w: $.width._4,      h: height     },
    _5::              { w: $.width._5,      h: height     },
    _6::              { w: $.width._6,      h: height     },
    _8::              { w: $.width._8,      h: height     },

    full::            { w: $.width._1,      h: height     },

    half::            { w: $.width._2,      h: height     },

    one_third::       { w: $.width._3,      h: height     },
    two_thirds::      { w: $.width._3 * 2,  h: height     },

    one_quarter::     { w: $.width._4,      h: height     },
    two_quarters::    { w: $.width._4 * 2,  h: height     },
    three_quarters::  { w: $.width._4 * 3,  h: height     },

    one_fifth::       { w: $.width._5,      h: height     },
    two_fifths::      { w: $.width._5 * 2,  h: height     },
    three_fifths::    { w: $.width._5 * 3,  h: height     },
    four_fifths::     { w: $.width._5 * 4,  h: height     },

    one_sixth::       { w: $.width._6,      h: height     },
    two_sixths::      { w: $.width._6 * 2,  h: height     },
    three_sixths::    { w: $.width._6 * 2,  h: height     },
    four_sixths::     { w: $.width._6 * 2,  h: height     },
    five_sixths::     { w: $.width._6 * 2,  h: height     },

    one_heighth::     { w: $.width._8,      h: height     },
    two_heighth::     { w: $.width._8 * 2,  h: height     },
    three_heighth::   { w: $.width._8 * 3,  h: height     },
    four_heighth::    { w: $.width._8 * 4,  h: height     },
    five_heighth::    { w: $.width._8 * 5,  h: height     },
    six_heighth::     { w: $.width._8 * 6,  h: height     },
    seven_heighth::   { w: $.width._8 * 7,  h: height     },

    title::           { w: $.width.full,        h: height / 2 },

    // TODO: Legacy items, remove in the future
     _3_4:            { w: $.width._4 * 3,  h: height     },
  },

  /**
   * @name layout.generate_grid
   *
   * Generate Grafana grid, based on panels size.
   * Should be called only once with the list of all panels that will be added.
   * It puts panels from left to right, until there is a free space, and then uses new line.
   * Line step on a new line break is the height of the last panel in a previous line.
   * You can use rows as line breaks.
   * For complex structures you can insert panels with pre-set position,
   * but you must be careful to not introduce conflicts.
   * If there are conflicts in grid, Grafana will solve them on import.
   * Grafana's negative gravity will also be applied on import, it is not computed here.
   * You should be careful with using both this and dynamic repeating with variables:
   * static generator do not know about dynamic repeating,
   * so Grafana conflict solver will be applied on import.
   *
   * @param panels List of panels (their width and height must be set in gridPos)
   *
   * @return List of panels with computed grid positions
   */
  generate_grid(panels):: [
    el {
      gridPos: {
        x: el.gridPos.x,
        y: el.gridPos.y,
        h: el.gridPos.h,
        w: el.gridPos.w,
      },
    }
    for el in std.foldl(function(_panels, p) (
      local i = std.length(_panels);
      local prev = (if i == 0 then null else _panels[i - 1]);

      if i == 0 then
        _panels + [p { gridPos: {
          x: 0,
          y: 0,
          h: p.gridPos.h,
          w: p.gridPos.w,
          x_cursor: p.gridPos.w,
          y_cursor: 0,
        } }]
      else
        local line_break = prev.gridPos.x_cursor + p.gridPos.w > grid_width;

        _panels + [p { gridPos: {
          x:
            if std.objectHas(p.gridPos, 'x') then
              p.gridPos.x
            else if line_break then
              0
            else
              prev.gridPos.x_cursor,

          y:
            if std.objectHas(p.gridPos, 'y') then
              p.gridPos.y
            else if line_break then
              prev.gridPos.y_cursor + prev.gridPos.h
            else
              prev.gridPos.y_cursor,

              h: p.gridPos.h,

              w: p.gridPos.w,

              x_cursor:
                if std.objectHas(p.gridPos, 'x') then
                  p.gridPos.x + p.gridPos.w
                else if line_break then
                  p.gridPos.w
                else
                  prev.gridPos.x_cursor + p.gridPos.w,

              y_cursor:
                if std.objectHas(p.gridPos, 'y') then
                  p.gridPos.y
                else if line_break then
                  prev.gridPos.y_cursor + prev.gridPos.h
                else
                  prev.gridPos.y_cursor,
        } }]
    ), panels, [])
  ],
}
