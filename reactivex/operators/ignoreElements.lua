local Observable = require 'reactivex.observable'
local Observer = require 'reactivex.observer'

--- Returns an Observable that terminates when the source terminates but does not produce any
-- elements.
-- @returns {Observable}
function Observable:ignoreElements()
  return self:lift(function (destination)
    local function onError(message)
      return destination:onError(message)
    end

    local function onCompleted()
      return destination:onCompleted()
    end

    return Observer.create(nil, onError, onCompleted)
  end)
end
