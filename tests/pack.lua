local Observable = require("reactivex.observable")
local Observer = require("reactivex.observer")
local Subscription = require("reactivex.subscription")

require('reactivex.operators.pack')

describe('pack', function()
  it('produces an error if its parent errors', function()
    createSingleUseOperator(
      "simulateError", 
      function (destination)
        destination:onError()
      end
    )
    local observable = Observable.of(''):simulateError()
    expect(observable).to.produce.error()
    expect(observable:pack()).to.produce.error()
  end)

  it('wraps elements of the source in tables', function()
    local observable = Observable.fromTable({4, 5, 6}, ipairs, true):pack()
    expect(observable).to.produce({{{4, 1, n = 2}}, {{5, 2, n = 2}}, {{6, 3, n = 2}}})
  end)
end)
