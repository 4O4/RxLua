local Observable = require 'reactivex.observable'
local util = require 'reactivex.util'
local Observer = require 'reactivex.observer'

--- Returns a new Observable that produces elements until the predicate returns falsy.
-- @arg {function} predicate - The predicate used to continue production of values.
-- @returns {Observable}
function Observable:takeWhile(predicate)
  predicate = predicate or util.identity

  return self:lift(function (destination)
    local taking = true

    local function onNext(...)
      if taking then
        util.tryWithObserver(destination, function(...)
          taking = predicate(...)
        end, ...)

        if taking then
          return destination:onNext(...)
        else
          return destination:onCompleted()
        end
      end
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
