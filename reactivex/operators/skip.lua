local Observable = require 'reactivex.observable'
local Observer = require 'reactivex.observer'

--- Returns a new Observable that skips over a specified number of values produced by the original
-- and produces the rest.
-- @arg {number=1} n - The number of values to ignore.
-- @returns {Observable}
function Observable:skip(n)
  n = n or 1

  return self:lift(function (destination)
    local i = 1

    local function onNext(...)
      if i > n then
        destination:onNext(...)
      else
        i = i + 1
      end
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
