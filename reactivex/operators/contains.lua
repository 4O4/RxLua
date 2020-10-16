local Observable = require 'reactivex.observable'
local util = require 'reactivex.util'
local Observer = require 'reactivex.observer'

--- Returns a new Observable that produces a single boolean value representing whether or not the
-- specified value was produced by the original.
-- @arg {*} value - The value to search for.  == is used for equality testing.
-- @returns {Observable}
function Observable:contains(value)
  return self:lift(function (destination)
    local function onNext(...)
      local args = util.pack(...)

      if #args == 0 and value == nil then
        destination:onNext(true)
        return destination:onCompleted()
      end

      for i = 1, #args do
        if args[i] == value then
          destination:onNext(true)
          return destination:onCompleted()
        end
      end
    end

    local function onError(e)
      return destination:onError(e)
    end

    local function onCompleted()
      destination:onNext(false)
      return destination:onCompleted()
    end

    return Observer.create(onNext, onError, onCompleted)
  end)
end
