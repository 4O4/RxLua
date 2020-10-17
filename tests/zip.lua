local Observable = require("reactivex.observable")
local Observer = require("reactivex.observer")
local Subscription = require("reactivex.subscription")

require('reactivex.operators.zip')

describe('zip', function()
  it('behaves as an identity function if only one Observable argument is specified', function()
    expect(Observable.fromRange(1, 5):zip()).to.produce(1, 2, 3, 4, 5)
  end)

  it('unsubscribes from all input observables', function()
    local unsubscribeA = spy()
    local subscriptionA = Subscription.create(unsubscribeA)
    local observableA = Observable.create(function(observer)
      return subscriptionA
    end)

    local unsubscribeB = spy()
    local subscriptionB = Subscription.create(unsubscribeB)
    local observableB = Observable.create(function(observer)
      return subscriptionB
    end)

    local subscription = Observable.zip(observableA, observableB):subscribe()
    subscription:unsubscribe()
    expect(#unsubscribeA).to.equal(1)
    expect(#unsubscribeB).to.equal(1)
  end)

  it('groups values produced by the sources by their index', function()
    local observableA = Observable.fromRange(1, 3)
    local observableB = Observable.fromRange(2, 4)
    local observableC = Observable.fromRange(3, 5)
    expect(Observable.zip(observableA, observableB, observableC)).to.produce({{1, 2, 3}, {2, 3, 4}, {3, 4, 5}})
  end)

  it('tolerates nils', function()
    local observableA = Observable.create(function(observer)
      observer:onNext(nil)
      observer:onNext(nil)
      observer:onNext(nil)
      observer:onCompleted()
    end)
    local observableB = Observable.fromRange(3)
    local onNext = observableSpy(Observable.zip(observableA, observableB))
    expect(#onNext).to.equal(3)
  end)
end)
