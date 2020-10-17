local Observable = require("reactivex.observable")
local Observer = require("reactivex.observer")
local Subscription = require("reactivex.subscription")

require('reactivex.operators.partition')

describe('partition', function()
  it('errors when its parent errors', function()
    createSingleUseOperator(
      "simulateError", 
      function (destination)
        destination:onError()
      end
    )
    local observable = Observable.of(''):simulateError()
    expect(observable).to.produce.error()
    expect(observable:partition()).to.produce.error()
  end)

  it('uses the identity function as the predicate if none is specified', function()
    local pass, fail = Observable.fromTable({true, false, true}):partition()
    expect(pass).to.produce(true, true)
    expect(fail).to.produce(false)
  end)

  it('partitions the elements into two observables based on the predicate', function()
    local function isEven(x) return x % 2 == 0 end
    local pass, fail = Observable.fromRange(5):partition(isEven)
    expect(pass).to.produce(2, 4)
    expect(fail).to.produce(1, 3, 5)
  end)
end)
