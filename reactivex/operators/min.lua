local Observable = require 'reactivex.observable'

require("reactivex.operators.reduce")

--- Returns a new Observable that produces the minimum value produced by the original.
-- @returns {Observable}
function Observable:min()
  return self:reduce(math.min)
end
