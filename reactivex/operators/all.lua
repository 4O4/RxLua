local Observable = require 'reactivex.observable'
local util = require 'reactivex.util'
local Observer = require 'reactivex.observer'

--- Determine whether all items emitted by an Observable meet some criteria.
-- @arg {function=identity} predicate - The predicate used to evaluate objects.
function Observable:all(predicate)
  predicate = predicate or util.identity

  return self:lift(function (destination)
    local function onNext(...)
      util.tryWithObserver(destination, function(...)
        if not predicate(...) then
          destination:onNext(false)
          destination:onCompleted()
        end
      end, ...)
    end

    local function onError(e)
      return destination:onError(e)
    end

    local function onCompleted()
      destination:onNext(true)
      return destination:onCompleted()
    end

    return Observer.create(onNext, onError, onCompleted)
  end)
end
