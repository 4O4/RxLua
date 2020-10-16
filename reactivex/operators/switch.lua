local Observable = require 'reactivex.observable'
local Subscription = require 'reactivex.subscription'
local Observer = require 'reactivex.observer'

--- Given an Observable that produces Observables, returns an Observable that produces the values
-- produced by the most recently produced Observable.
-- @returns {Observable}
function Observable:switch()
  return self:lift(function (destination)
    local innerSubscription
    local sink

    local function onNext(...)
      return destination:onNext(...)
    end

    local function onError(message)
      return destination:onError(message)
    end

    local function onCompleted()
      return destination:onCompleted()
    end

    local function switch(source)
      if innerSubscription then
        innerSubscription:unsubscribe()
        sink:remove(innerSubscription)
      end

      innerSubscription = source:subscribe(onNext, onError, nil)
      sink:add(innerSubscription)
    end

    sink = Observer.create(switch, onError, onCompleted)

    return sink
  end)
end
