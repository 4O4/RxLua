local Observable = require 'reactivex.observable'
local Observer = require 'reactivex.observer'

--- Returns an Observable that takes any values produced by the original that consist of multiple
-- return values and produces each value individually.
-- @returns {Observable}
function Observable:unwrap()
  return self:lift(function (destination)
    local function onNext(...)
      local values = {...}
      for i = 1, #values do
        destination:onNext(values[i])
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
