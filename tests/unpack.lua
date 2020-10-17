local Observable = require("reactivex.observable")
local Observer = require("reactivex.observer")
local Subscription = require("reactivex.subscription")

require('reactivex.operators.unpack')

describe('unpack', function()
  it('produces an error if its parent errors', function()
    createSingleUseOperator(
      "simulateError", 
      function (destination)
        destination:onError()
      end
    )
    local observable = Observable.of(''):simulateError()
    expect(observable).to.produce.error()
    expect(observable:unpack()).to.produce.error()
  end)

  it('fails if the observable produces an element that is not a table', function()
    expect(Observable.of(3):unpack()).to.produce.error()
  end)

  it('produces all elements in the tables produced as multiple values', function()
    local observable = Observable.fromTable({{1, 2, 3}, {4, 5, 6}}, ipairs):unpack()
    expect(observable).to.produce({{1, 2, 3}, {4, 5, 6}})
  end)
end)
