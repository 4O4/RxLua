local Observable = require 'reactivex.observable'
local util = require 'reactivex.util'
local Observer = require 'reactivex.observer'

--- Returns an Observable that produces a sliding window of the values produced by the original.
-- @arg {number} size - The size of the window. The returned observable will produce this number
--                      of the most recent values as multiple arguments to onNext.
-- @returns {Observable}
function Observable:window(size)
  if not size or type(size) ~= 'number' then
    error('Expected a number')
  end

  return self:lift(function (destination)
    local window = {}

    local function onNext(value)
      table.insert(window, value)

      if #window >= size then
        destination:onNext(util.unpack(window))
        table.remove(window, 1)
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
