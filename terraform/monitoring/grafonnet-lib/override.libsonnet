{
  new(
    name,
    matcher     = $.matcher.field,
    properties  = [],
  )::
  {
    matcher: {
      id:       matcher,
      options:  name
    },
    properties: properties
  },

  matcher:: {
    name::    'byName',
    regexp::  'byRegexp',
    type::    'byType',
    field::   'byFrameRefID',
  },
}
