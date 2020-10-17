local Observable = require("reactivex.observable")
local Observer = require("reactivex.observer")
local Subscription = require("reactivex.subscription")

require('reactivex.operators.average')

describe('average', function()
  it('errors when its parent errors', function()
    expect(Observable.throw():average()).to.produce.error()
  end)

  it('produces a single value representing the average of the values produced by the source', function()
    expect(Observable.fromRange(3, 9, 2):average()).to.produce(6)
    expect(Observable.fromTable({-1, 0, 1}):average()).to.produce(0)
  end)

  it('produces nothing if there are no values to average', function()
    expect(Observable.empty():average()).to.produce.nothing()
  end)
end)
