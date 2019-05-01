describe('buffer', function()
  it('produces an error if its parent errors', function()
    expect(Rx.Observable.throw():buffer(1)).to.produce.error()
  end)

  it('fails if size is not specified', function()
    expect(function () Rx.Observable.fromRange(5):buffer() end).to.fail()
  end)

  it('produces values wrapped to the specified width', function()
    local observable = Rx.Observable.create(function(observer)
      observer:onNext(1)
      observer:onNext(2, 3)
      observer:onNext(4, 5, 6)
      observer:onCompleted()
    end)
    expect(observable).to.produce({{1}, {2, 3}, {4, 5, 6}})
    expect(observable:buffer(2)).to.produce({{1, 2}, {3, 4}, {5, 6}})
  end)

  it('produces a partial buffer if the observable completes', function()
    local observable = Rx.Observable.fromRange(5)
    expect(observable:buffer(2)).to.produce({{1, 2}, {3, 4}, {5}})
  end)

  it('produces a partial buffer if the observable errors', function()
    local observable = Rx.Observable.create(function(observer)
      observer:onNext(1)
      observer:onNext(2)
      observer:onNext(3)
      observer:onError('oops')
    end)
    local onNext, onError = observableSpy(observable:buffer(2))
    expect(onNext).to.equal({{1, 2}, {3}})
    expect(#onError).to.equal(1)
  end)
end)
