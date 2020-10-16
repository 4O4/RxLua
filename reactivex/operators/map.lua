local Observable = require 'reactivex.observable'
local util = require 'reactivex.util'
local Observer = require 'reactivex.observer'

--- Returns a new Observable that produces the values of the original transformed by a function.
-- @arg {function} callback - The function to transform values from the original Observable.
-- @returns {Observable}
function Observable:map(callback)
  return self:lift(function (destination)
    callback = callback or util.identity

    local function onNext(...)
      return util.tryWithObserver(destination, function(...)
        return destination:onNext(callback(...))
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
