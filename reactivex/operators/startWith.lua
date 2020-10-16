local Observable = require 'reactivex.observable'
local util = require 'reactivex.util'

--- Returns a new Observable that produces the specified values followed by all elements produced by
-- the source Observable.
-- @arg {*...} values - The values to produce before the Observable begins producing values
--                      normally.
-- @returns {Observable}
function Observable:startWith(...)
  local values = util.pack(...)
  return self:lift(function (destination)
    destination:onNext(util.unpack(values))
    return destination
  end)
end
