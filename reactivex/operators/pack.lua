local Observable = require 'reactivex.observable'
local util = require 'reactivex.util'

require("reactivex.operators.map")

--- Returns an Observable that produces the values of the original inside tables.
-- @returns {Observable}
function Observable:pack()
  return self:map(util.pack)
end
