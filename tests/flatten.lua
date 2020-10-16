describe('flatten', function()
  it('produces an error if its parent errors', function()
    local observable = Rx.Observable.of(''):map(function(x) return x() end)
    expect(observable).to.produce.error()
    expect(observable:flatten()).to.produce.error()
  end)

  it('produces all values produced by the observables produced by its parent', function()
    local observable = Rx.Observable.fromRange(3):map(function(i)
      return Rx.Observable.fromRange(i, 3)
    end):flatten()

    expect(observable).to.produce(1, 2, 3, 2, 3, 3)
  end)

  it('should unsubscribe from all source observables', function()
    local subA = Rx.Subscription.create()
    local observableA = Rx.Observable.create(function(observer)
      return subA
    end)

    local subB = Rx.Subscription.create()
    local observableB = Rx.Observable.create(function(observer)
      return subB
    end)

    local subject = Rx.Observable.create(function (observer)
      observer:onNext(observableA)
      observer:onNext(observableB)
    end)
    local subscription = subject:flatten():subscribe()

    subscription:unsubscribe()

    expect(subA:isUnsubscribed()).to.equal(true)
    expect(subB:isUnsubscribed()).to.equal(true)
  end)
end)
