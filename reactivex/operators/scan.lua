local Observable = require 'reactivex.observable'
local util = require 'reactivex.util'
local Observer = require 'reactivex.observer'

--- Returns a new Observable that produces values computed by accumulating the results of running a
-- function on each value produced by the original Observable.
-- @arg {function} accumulator - Accumulates the values of the original Observable. Will be passed
--                               the return value of the last call as the first argument and the
--                               current values as the rest of the arguments.  Each value returned
--                               from this function will be emitted by the Observable.
-- @arg {*} seed - A value to pass to the accumulator the first time it is run.
-- @returns {Observable}
function Observable:scan(accumulator, seed)
  return self:lift(function (destination)
    local result = seed
    local first = true

    local function onNext(...)
      if first and seed == nil then
        result = ...
        first = false
      else
        return util.tryWithObserver(destination, function(...)
          result = accumulator(result, ...)
          destination:onNext(result)
        end, ...)
      end
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
