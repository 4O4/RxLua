local Observable = require("reactivex.observable")
local Observer = require("reactivex.observer")
local Subscription = require("reactivex.subscription")

require('reactivex.operators.first')

describe('first', function()
  it('produces an error if its parent errors', function()
    createSingleUseOperator(
      "simulateError", 
      function (destination)
        destination:onError()
      end
    )
    local observable = Observable.of(''):simulateError()
    expect(observable).to.produce.error()
    expect(observable:first()).to.produce.error()
  end)

  it('produces no elements if its parent produces no elements', function()
    local observable = Observable.create(function(observer) return observer:onCompleted() end):first()
    expect(observable).to.produce.nothing()
  end)

  it('produces the first element of its parent and immediately completes', function()
    local observable = Observable.fromRange(5):first()
    expect(observable).to.produce(1)
  end)
end)
