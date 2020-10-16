local Observable = require 'reactivex.observable'
local Subscription = require 'reactivex.subscription'
local util = require 'reactivex.util'
local Observer = require 'reactivex.observer'

--- Returns a new Observable that subscribes to the Observables produced by the original and
-- produces their values.
-- @returns {Observable}
function Observable:flatten()
  return self:lift(function (destination)
    local sink

    local function onError(message)
      return destination:onError(message)
    end

    local function onNext(observable)
      local function innerOnNext(...)
        destination:onNext(...)
      end

      sink:add(observable:subscribe(innerOnNext, onError, util.noop))
    end

    local function onCompleted()
      return destination:onCompleted()
    end

    sink = Observer.create(onNext, onError, onCompleted)

    return sink
  end)
end
