local Observable = require 'reactivex.observable'
local util = require 'reactivex.util'
local Observer = require 'reactivex.observer'

--- Returns a new Observable that produces a default set of items if the source Observable produces
-- no values.
-- @arg {*...} values - Zero or more values to produce if the source completes without emitting
--                      anything.
-- @returns {Observable}
function Observable:defaultIfEmpty(...)
  local defaults = util.pack(...)

  return self:lift(function (destination)
    local hasValue = false

    local function onNext(...)
      hasValue = true
      destination:onNext(...)
    end

    local function onError(e)
      destination:onError(e)
    end

    local function onCompleted()
      if not hasValue then
        destination:onNext(util.unpack(defaults))
      end

      destination:onCompleted()
    end

    return Observer.create(onNext, onError, onCompleted)
  end)
end
