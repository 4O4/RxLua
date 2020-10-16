local Observable = require 'reactivex.observable'
local util = require 'reactivex.util'
local Observer = require 'reactivex.observer'

--- Returns a new Observable that produces the first value of the original that satisfies a
-- predicate.
-- @arg {function} predicate - The predicate used to find a value.
function Observable:find(predicate)
  predicate = predicate or util.identity

  return self:lift(function (destination)
    local function onNext(...)
      util.tryWithObserver(destination, function(...)
        if predicate(...) then
          destination:onNext(...)
          return destination:onCompleted()
        end
      end, ...)
    end

    local function onError(message)
      return destination:onError(message)
    end

    local function onCompleted()
      return destination:onCompleted()
    end

    return Observer.create(onNext, onError, onCompleted)
  end)
end
