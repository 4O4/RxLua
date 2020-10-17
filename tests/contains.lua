local Observable = require("reactivex.observable")
local Observer = require("reactivex.observer")
local Subscription = require("reactivex.subscription")

require('reactivex.operators.contains')
require('reactivex.operators.buffer')

describe('contains', function()
  it('errors when its parent errors', function()
    expect(Observable.throw():contains(1)).to.produce.error()
  end)

  it('returns false if the source Observable produces no values', function()
    expect(Observable.empty():contains(3)).to.produce(false)
  end)

  it('returns true if the value is nil and the Observable produces an empty value', function()
    local observable = Observable.create(function(observer)
      observer:onNext(nil)
      observer:onCompleted()
    end)

    expect(observable:contains(nil)).to.produce(true)
  end)

  it('returns true if the source Observable produces the specified value', function()
    local observable = Observable.fromRange(5)
    expect(observable:contains(3)).to.produce(true)
  end)

  it('supports multiple values', function()  -- FIXME: ???
    local observable = Observable.fromRange(6):buffer(3)
    expect(observable).to.produce({{1, 2, 3}, {4, 5, 6}})
    expect(observable:contains(5)).to.produce(true)
  end)
end)
