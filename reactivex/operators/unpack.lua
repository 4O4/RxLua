local Observable = require 'reactivex.observable'
local util = require 'reactivex.util'

require("reactivex.operators.map")

--- Returns an Observable that unpacks the tables produced by the original.
-- @returns {Observable}
function Observable:unpack()
  return self:map(util.unpack)
end
