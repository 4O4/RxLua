local Observable = require 'reactivex.observable'

require('reactivex.operators.take')

--- Returns a new Observable that only produces the first result of the original.
-- @returns {Observable}
function Observable:first()
  return self:take(1)
end
