local Observable = require("reactivex.observable")
local Observer = require("reactivex.observer")
local Subscription = require("reactivex.subscription")

require('reactivex.operators.filter')

describe('filter', function()
  it('uses the identity function as the predicate if none is specified', function()
    local observable = Observable.of(false, true):filter()
    expect(observable).to.produce(true)
  end)

  it('passes all arguments to the predicate', function()
    local predicateSpy = spy()
    local observable = Observable.create(function (observer)
      print("emitted")
      observer:onNext(1, 2)
      observer:onNext(3, 4, 5)
    end)
    
    observable:filter(predicateSpy):subscribe()

    expect(predicateSpy).to.equal({{1, 2}, {3, 4, 5}})
  end)

  it('does not produce elements that the predicate returns false for', function()
    local predicate = function(x) return x % 2 == 0 end
    local observable = Observable.fromRange(1, 5):filter(predicate)
    expect(observable).to.produce(2, 4)
  end)

  it('errors when its parent errors', function()
    expect(Observable.throw():filter()).to.produce.error()
  end)

  it('calls onError if the predicate errors', function()
    expect(Observable.of(5):filter(error)).to.produce.error()
  end)
end)
