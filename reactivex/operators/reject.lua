local Observable = require 'reactivex.observable'
local util = require 'reactivex.util'
local Observer = require 'reactivex.observer'

--- Returns a new Observable that produces values from the original which do not satisfy a
-- predicate.
-- @arg {function} predicate - The predicate used to reject values.
-- @returns {Observable}
function Observable:reject(predicate)
  predicate = predicate or util.identity

  return self:lift(function (destination)
    local function onNext(...)
      util.tryWithObserver(destination, function(...)
        if not predicate(...) then
          return destination:onNext(...)
        end
      end, ...)
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
