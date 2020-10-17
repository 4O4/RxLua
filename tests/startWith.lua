local Observable = require("reactivex.observable")
local Observer = require("reactivex.observer")
local Subscription = require("reactivex.subscription")

require('reactivex.operators.startWith')

describe('startWith', function()
  it('produces errors emitted by the source', function()
    expect(Observable.throw():startWith(1)).to.produce.error()
  end)

  it('produces all specified elements in a single onNext before producing values normally', function()
    expect(Observable.fromRange(3, 4):startWith(1, 2)).to.produce({{1, 2}, {3}, {4}})
  end)
end)
