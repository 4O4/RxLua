local Observable = require 'reactivex.observable'
local Observer = require 'reactivex.observer'

--- Returns a new Observable that skips over values produced by the original until the specified
-- Observable produces a value.
-- @arg {Observable} other - The Observable that triggers the production of values.
-- @returns {Observable}
function Observable:skipUntil(other)
  return self:lift(function (destination)
    local triggered = false
    local function trigger()
      triggered = true
    end

    other:subscribe(trigger, trigger, trigger)

    local function onNext(...)
      if triggered then
        destination:onNext(...)
      end
    end

    local function onError()
      if triggered then
        destination:onError()
      end
    end

    local function onCompleted()
      if triggered then
        destination:onCompleted()
      end
    end

    return Observer.create(onNext, onError, onCompleted)
  end)
end
