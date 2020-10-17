local Observable = require("reactivex.observable")
local Observer = require("reactivex.observer")
local Subscription = require("reactivex.subscription")

require('reactivex.operators.flatten')

describe('flatten', function()
  it('produces an error if its parent errors', function()
    createSingleUseOperator(
      "simulateError", 
      function (destination)
        destination:onError()
      end
    )
    local observable = Observable.of(''):simulateError()
    expect(observable).to.produce.error()
    expect(observable:flatten()).to.produce.error()
  end)

  it('produces all values produced by the observables produced by its parent', function()
    createSingleUseOperator(
      "produceTestObservable", 
      function (destination)
        local function onNext(value)
          destination:onNext(Observable.fromRange(value, 3))
        end

        return createPassThroughObserver(destination, onNext)
      end
    )
    local observable = Observable.fromRange(1, 3):produceTestObservable():flatten()

    expect(observable).to.produce(1, 2, 3, 2, 3, 3)
  end)

  it('should unsubscribe from all source observables', function()
    local subA = Subscription.create()
    local observableA = Observable.create(function(observer)
      return subA
    end)

    local subB = Subscription.create()
    local observableB = Observable.create(function(observer)
      return subB
    end)

    local subject = Observable.create(function (observer)
      observer:onNext(observableA)
      observer:onNext(observableB)
    end)
    local subscription = subject:flatten():subscribe()

    subscription:unsubscribe()

    expect(subA:isUnsubscribed()).to.equal(true)
    expect(subB:isUnsubscribed()).to.equal(true)
  end)
end)
