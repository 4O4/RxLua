local Observable = require 'reactivex.observable'
local util = require 'reactivex.util'
local Observer = require 'reactivex.observer'

--- Returns an Observable that produces a specified number of elements from the end of a source
-- Observable.
-- @arg {number} count - The number of elements to produce.
-- @returns {Observable}
function Observable:takeLast(count)
  if not count or type(count) ~= 'number' then
    error('Expected a number')
  end

  return self:lift(function (destination)
    local buffer = {}

    local function onNext(...)
      table.insert(buffer, util.pack(...))
      if #buffer > count then
        table.remove(buffer, 1)
      end
    end

    local function onError(message)
      return destination:onError(message)
    end

    local function onCompleted()
      for i = 1, #buffer do
        destination:onNext(util.unpack(buffer[i]))
      end
      return destination:onCompleted()
    end

    return Observer.create(onNext, onError, onCompleted)
  end)
end
