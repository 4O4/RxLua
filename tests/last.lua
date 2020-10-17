local Observable = require("reactivex.observable")
local Observer = require("reactivex.observer")
local Subscription = require("reactivex.subscription")

require('reactivex.operators.last')

describe('last', function()
  it('produces an error if its parent errors', function()
    createSingleUseOperator(
      "simulateError", 
      function (destination)
        destination:onError()
      end
    )
    local observable = Observable.of(''):simulateError()
    expect(observable).to.produce.error()
    expect(observable:last()).to.produce.error()
  end)

  it('produces no elements if its parent produces no elements', function()
    local observable = Observable.create(function(observer) return observer:onCompleted() end):last()
    expect(observable).to.produce.nothing()
  end)

  it('produces the last element of its parent and immediately completes', function()
    local observable = Observable.fromRange(5):last()
    expect(observable).to.produce(5)
  end)
end)
