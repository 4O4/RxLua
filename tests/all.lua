local Observable = require("reactivex.observable")
local Observer = require("reactivex.observer")
local Subscription = require("reactivex.subscription")

require('reactivex.operators.all')

describe('all', function()
  it('passes through errors', function()
    expect(Observable.throw():all()).to.produce.error()
  end)

  it('calls onError if the predicate errors', function()
    expect(Observable.fromRange(3):all(error)).to.produce.error()
  end)

  it('produces an error if the parent errors', function()
    expect(Observable.throw():all(function(x) return x end)).to.produce.error()
  end)

  it('produces true if all elements satisfy the predicate', function()
    local observable = Observable.fromRange(5):all(function(x) return x < 10 end)
    expect(observable).to.produce({{true}})
  end)

  it('produces false if one element does not satisfy the predicate', function()
    local observable = Observable.fromRange(5):all(function(x) return x ~= 3 end)
    expect(observable).to.produce({{false}})
  end)

  it('uses the identity function as a predicate if none is specified', function()
    local observable = Observable.of(false):all()
    expect(observable).to.produce({{false}})

    observable = Observable.of(true):all()
    expect(observable).to.produce({{true}})
  end)
end)
