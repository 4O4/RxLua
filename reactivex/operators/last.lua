local Observable = require 'reactivex.observable'
local util = require 'reactivex.util'
local Observer = require 'reactivex.observer'

--- Returns a new Observable that only produces the last result of the original.
-- @returns {Observable}
function Observable:last()
  return self:lift(function (destination)
    local value
    local empty = true

    local function onNext(...)
      value = {...}
      empty = false
    end

    local function onError(e)
      return destination:onError(e)
    end

    local function onCompleted()
      if not empty then
        destination:onNext(util.unpack(value or {}))
      end

      return destination:onCompleted()
    end

    return Observer.create(onNext, onError, onCompleted)
  end)
end
