local Observable = require 'reactivex.observable'
local util = require 'reactivex.util'
local Observer = require 'reactivex.observer'

--- Returns an Observable that only produces values from the original if they are different from
-- the previous value.
-- @arg {function} comparator - A function used to compare 2 values. If unspecified, == is used.
-- @returns {Observable}
function Observable:distinctUntilChanged(comparator)
  comparator = comparator or util.eq

  return self:lift(function (destination)
    local first = true
    local currentValue = nil

    local function onNext(value, ...)
      local values = util.pack(...)
      util.tryWithObserver(destination, function()
        if first or not comparator(value, currentValue) then
          destination:onNext(value, util.unpack(values))
          currentValue = value
          first = false
        end
      end)
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
