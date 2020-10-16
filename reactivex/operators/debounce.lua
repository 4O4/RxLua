local Observable = require 'reactivex.observable'
local Subscription = require 'reactivex.subscription'
local util = require 'reactivex.util'
local Observer = require 'reactivex.observer'

--- Returns a new throttled Observable that waits to produce values until a timeout has expired, at
-- which point it produces the latest value from the source Observable.  Whenever the source
-- Observable produces a value, the timeout is reset.
-- @arg {number|function} time - An amount in milliseconds to wait before producing the last value.
-- @arg {Scheduler} scheduler - The scheduler to run the Observable on.
-- @returns {Observable}
function Observable:debounce(time, scheduler)
  time = time or 0

  return self:lift(function (destination)
    local debounced = {}
    local sink

    local function wrap(key)
      return function(...)
        local value = util.pack(...)

        if debounced[key] then
          debounced[key]:unsubscribe()
          sink:remove(debounced[key])
        end

        local values = util.pack(...)

        debounced[key] = scheduler:schedule(function()
          return destination[key](destination, util.unpack(values))
        end, time)
        sink:add(debounced[key])
      end
    end

    sink = Observer.create(wrap('onNext'), wrap('onError'), wrap('onCompleted'))

    return sink
  end)
end
