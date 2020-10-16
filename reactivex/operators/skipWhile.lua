local Observable = require 'reactivex.observable'
local util = require 'reactivex.util'
local Observer = require 'reactivex.observer'

--- Returns a new Observable that skips elements until the predicate returns falsy for one of them.
-- @arg {function} predicate - The predicate used to continue skipping values.
-- @returns {Observable}
function Observable:skipWhile(predicate)
  predicate = predicate or util.identity

  return self:lift(function (destination)
    local skipping = true

    local function onNext(...)
      if skipping then
        util.tryWithObserver(destination, function(...)
          skipping = predicate(...)
        end, ...)
      end

      if not skipping then
        return destination:onNext(...)
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
