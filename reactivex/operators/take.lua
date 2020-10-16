local Observable = require 'reactivex.observable'
local Observer = require 'reactivex.observer'

--- Returns a new Observable that only produces the first n results of the original.
-- @arg {number=1} n - The number of elements to produce before completing.
-- @returns {Observable}
function Observable:take(n)
  local n = n or 1

  return self:lift(function (destination)
    if n <= 0 then
      destination:onCompleted()
      return
    end

    local i = 1

    local function onNext(...)
      destination:onNext(...)

      i = i + 1

      if i > n then
        destination:onCompleted()
        destination:unsubscribe()
      end
    end

    local function onError(e)
      destination:onError(e)
    end

    local function onCompleted()
      destination:onCompleted()
    end

    return Observer.create(onNext, onError, onCompleted)
  end)
end
