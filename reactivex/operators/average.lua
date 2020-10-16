local Observable = require 'reactivex.observable'
local Observer = require 'reactivex.observer'

--- Returns an Observable that produces the average of all values produced by the original.
-- @returns {Observable}
function Observable:average()
  return self:lift(function (destination)
    local sum, count = 0, 0

    local function onNext(value)
      sum = sum + value
      count = count + 1
    end

    local function onError(e)
      destination:onError(e)
    end

    local function onCompleted()
      if count > 0 then
        destination:onNext(sum / count)
      end

      destination:onCompleted()
    end

    return Observer.create(onNext, onError, onCompleted)
  end)
end
