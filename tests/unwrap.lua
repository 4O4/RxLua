local Observable = require("reactivex.observable")
local Observer = require("reactivex.observer")
local Subscription = require("reactivex.subscription")

require('reactivex.operators.unwrap')

describe('unwrap', function()
  it('produces an error if its parent errors', function()
    createSingleUseOperator(
      "simulateError", 
      function (destination)
        destination:onError()
      end
    )
    local observable = Observable.of(''):simulateError()
    expect(observable).to.produce.error()
    expect(observable:unwrap()).to.produce.error()
  end)

  it('produces any multiple values as individual values', function()
    local observable = Observable.create(function(observer)
      observer:onNext(1)
      observer:onNext(2, 3)
      observer:onNext(4, 5, 6)
      observer:onCompleted()
    end)
    expect(observable).to.produce({{1}, {2, 3}, {4, 5, 6}})
    expect(observable:unwrap()).to.produce(1, 2, 3, 4, 5, 6)
  end)
end)
