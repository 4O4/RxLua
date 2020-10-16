local Observable = require 'reactivex.observable'
local util = require 'reactivex.util'
local Observer = require 'reactivex.observer'

--- Returns an Observable that produces a single value representing the number of values produced
-- by the source value that satisfy an optional predicate.
-- @arg {function=} predicate - The predicate used to match values.
function Observable:count(predicate)
  predicate = predicate or util.constant(true)

  return self:lift(function (destination)
    local count = 0

    local function onNext(...)
      util.tryWithObserver(destination, function(...)
        if predicate(...) then
          count = count + 1
        end
      end, ...)
    end

    local function onError(e)
      return destination:onError(e)
    end

    local function onCompleted()
      destination:onNext(count)
      destination:onCompleted()
    end

    return Observer.create(onNext, onError, onCompleted)
  end)
end
