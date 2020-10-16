local Observable = require 'reactivex.observable'
local Observer = require 'reactivex.observer'

--- Returns a new Observable that produces the values from the original with duplicates removed.
-- @returns {Observable}
function Observable:distinct()
  return self:lift(function (destination)
    local values = {}

    local function onNext(x)
      if not values[x] then
        destination:onNext(x)
      end

      values[x] = true
    end

    local function onError(e)
      return destination:onError(e)
    end

    local function onCompleted()
      return destination:onCompleted()
    end

    return Observer.create(onNext, onError, onCompleted)
  end)
end
