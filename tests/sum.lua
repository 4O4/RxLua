local Observable = require("reactivex.observable")
local Observer = require("reactivex.observer")
local Subscription = require("reactivex.subscription")

require('reactivex.operators.sum')

describe('sum', function()
  it('produces an error if its parent errors', function()
    createSingleUseOperator(
      "simulateError", 
      function (destination)
        destination:onError()
      end
    )
    local observable = Observable.of(''):simulateError()
    expect(observable).to.produce.error()
    expect(observable:sum()).to.produce.error()
  end)

  it('produces the sum of the numeric values from the source', function()
    expect(Observable.fromRange(3):sum()).to.produce(6)
  end)
end)
