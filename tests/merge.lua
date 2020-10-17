local Observable = require("reactivex.observable")
local Observer = require("reactivex.observer")
local Subscription = require("reactivex.subscription")
local Subject = require("reactivex.subjects.subject")

require('reactivex.operators.merge')

describe('merge', function()
  it('produces values from the first observable if it is the only argument', function()
    local observable = Observable.fromRange(5):merge()
    expect(observable).to.produce(1, 2, 3, 4, 5)
  end)

  it('unsubscribes from all input observables', function()
    local observableA = Observable.create(function(observer)
      return
    end)

    local unsubscribeB = spy()
    local subscriptionB = Subscription.create(unsubscribeB)
    local observableB = Observable.create(function(observer)
      return subscriptionB
    end)

    local subscription = observableA:merge(observableB):subscribe()
    subscription:unsubscribe()
    expect(#unsubscribeB).to.equal(1)
  end)

  it('unsubscribes from all input observables included completed', function()
    local observableA = Observable.empty()

    local unsubscribeB = spy()
    local subscriptionB = Subscription.create(unsubscribeB)
    local observableB = Observable.create(function(observer)
      return subscriptionB
    end)

    local subscription = observableA:merge(Observable.empty(), observableB):subscribe()
    subscription:unsubscribe()
    expect(#unsubscribeB).to.equal(1)
  end)

  it('produces values from all input observables, in order', function()
    local observableA = Subject.create()
    local observableB = Subject.create()
    local merged = observableA:merge(observableB)
    local onNext, onError, onCompleted = observableSpy(merged)
    observableA:onNext('a')
    observableB:onNext('b')
    observableB:onNext('b')
    observableA:onNext('a')
    observableA:onCompleted()
    observableB:onCompleted()
    expect(onNext).to.equal({{'a'}, {'b'}, {'b'}, {'a'}})
  end)

  it('completes when all source observables complete', function()
    local observableA = Subject.create()
    local observableB = Subject.create()
    local complete = spy()
    Observable.merge(observableA, observableB):subscribe(nil, nil, complete)

    expect(#complete).to.equal(0)
    observableA:onNext(1)
    expect(#complete).to.equal(0)
    observableB:onNext(2)
    expect(#complete).to.equal(0)
    observableB:onCompleted()
    expect(#complete).to.equal(0)
    observableA:onCompleted()
    expect(#complete).to.equal(1)
  end)
end)
