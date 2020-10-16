local Observable = require 'reactivex.observable'
local util = require 'reactivex.util'
local Observer = require 'reactivex.observer'

--- Returns an Observable that omits a specified number of values from the end of the original
-- Observable.
-- @arg {number} count - The number of items to omit from the end.
-- @returns {Observable}
function Observable:skipLast(count)
  if not count or type(count) ~= 'number' then
    error('Expected a number')
  end

  local buffer = {}
  return self:lift(function (destination)
    local function emit()
      if #buffer > count and buffer[1] then
        local values = table.remove(buffer, 1)
        destination:onNext(util.unpack(values))
      end
    end

    local function onNext(...)
      emit()
      table.insert(buffer, util.pack(...))
    end

    local function onError(message)
      return destination:onError(message)
    end

    local function onCompleted()
      emit()
      return destination:onCompleted()
    end

    return Observer.create(onNext, onError, onCompleted)
  end)
end
