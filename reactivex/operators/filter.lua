local Observable = require 'reactivex.observable'
local util = require 'reactivex.util'
local Observer = require 'reactivex.observer'

--- Returns a new Observable that only produces values of the first that satisfy a predicate.
-- @arg {function} predicate - The predicate used to filter values.
-- @returns {Observable}
function Observable:filter(predicate)
  predicate = predicate or util.identity

  return self:lift(function (destination)
    local function onNext(...)
      util.tryWithObserver(destination, function(...)
        if predicate(...) then
          destination:onNext(...)
          return
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
