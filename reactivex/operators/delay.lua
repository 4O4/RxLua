local Observable = require 'reactivex.observable'
local Subscription = require 'reactivex.subscription'
local util = require 'reactivex.util'
local Observer = require 'reactivex.observer'

--- Returns a new Observable that produces the values of the original delayed by a time period.
-- @arg {number|function} time - An amount in milliseconds to delay by, or a function which returns
--                                this value.
-- @arg {Scheduler} scheduler - The scheduler to run the Observable on.
-- @returns {Observable}
function Observable:delay(time, scheduler)
  time = type(time) ~= 'function' and util.constant(time) or time

  return self:lift(function (destination)
    local sink

    local function delay(key)
      return function(...)
        local arg = util.pack(...)
        sink:add(scheduler:schedule(function()
          destination[key](destination, util.unpack(arg))
        end, time()))
      end
    end

    sink = Observer.create(delay('onNext'), delay('onError'), delay('onCompleted'))

    return sink
  end)
end
