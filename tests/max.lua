local Observable = require("reactivex.observable")
local Observer = require("reactivex.observer")
local Subscription = require("reactivex.subscription")

require('reactivex.operators.max')

describe('max', function()
  it('produces an error if its parent errors', function()
    createSingleUseOperator(
      "simulateError", 
      function (destination)
        destination:onError()
      end
    )
    local observable = Observable.of(''):simulateError()
    expect(observable).to.produce.error()
    expect(observable:max()).to.produce.error()
  end)

  it('produces an error if one of the values produced is a string', function()
    local observable = Observable.of(1, 'string'):max()
    expect(observable).to.produce.error()
  end)

  it('produces the maximum of all values produced', function()
    local observable = Observable.fromRange(5):max()
    expect(observable).to.produce(5)
  end)
end)
