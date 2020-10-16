local Observable = require 'reactivex.observable'
local util = require 'reactivex.util'
local Observer = require 'reactivex.observer'

--- Returns an Observable that buffers values from the original and produces them as multiple
-- values.
-- @arg {number} size - The size of the buffer.
function Observable:buffer(size)
  if not size or type(size) ~= 'number' then
    error('Expected a number')
  end

  return self:lift(function (destination)
    local buffer = {}

    local function emit()
      if #buffer > 0 then
        destination:onNext(util.unpack(buffer))
        buffer = {}
      end
    end

    local function onNext(...)
      local values = {...}
      for i = 1, #values do
        table.insert(buffer, values[i])
        if #buffer >= size then
          emit()
        end
      end
    end

    local function onError(message)
      emit()
      return destination:onError(message)
    end

    local function onCompleted()
      emit()
      return destination:onCompleted()
    end

    return Observer.create(onNext, onError, onCompleted)
  end)
end
