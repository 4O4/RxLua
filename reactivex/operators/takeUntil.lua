local Observable = require 'reactivex.observable'
local Observer = require 'reactivex.observer'

--- Returns a new Observable that completes when the specified Observable fires.
-- @arg {Observable} other - The Observable that triggers completion of the original.
-- @returns {Observable}
function Observable:takeUntil(other)
  return self:lift(function (destination)
    local function onNext(...)
      return destination:onNext(...)
    end

    local function onError(e)
      return destination:onError(e)
    end

    local function onCompleted()
      return destination:onCompleted()
    end

    other:subscribe(onCompleted, onCompleted, onCompleted)

    return Observer.create(onNext, onError, onCompleted)
  end)
end
