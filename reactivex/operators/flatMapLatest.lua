local Observable = require 'reactivex.observable'
local Subscription = require 'reactivex.subscription'
local util = require 'reactivex.util'
local Observer = require 'reactivex.observer'

--- Returns a new Observable that uses a callback to create Observables from the values produced by
-- the source, then produces values from the most recent of these Observables.
-- @arg {function=identity} callback - The function used to convert values to Observables.
-- @returns {Observable}
function Observable:flatMapLatest(callback)
  callback = callback or util.identity
  return self:lift(function (destination)
    local innerSubscription
    local sink

    local function onNext(...)
      destination:onNext(...)
    end

    local function onError(e)
      return destination:onError(e)
    end

    local function onCompleted()
      return destination:onCompleted()
    end

    local function subscribeInner(...)
      if innerSubscription then
        innerSubscription:unsubscribe()
        sink:remove(innerSubscription)
      end

      return util.tryWithObserver(destination, function(...)
        innerSubscription = callback(...):subscribe(onNext, onError)
        sink:add(innerSubscription)
      end, ...)
    end

    sink = Observer.create(subscribeInner, onError, onCompleted)
    return sink
  end)
end
