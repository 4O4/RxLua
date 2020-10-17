local Observable = require("reactivex.observable")
local Observer = require("reactivex.observer")
local Subscription = require("reactivex.subscription")

require('reactivex.operators.takeWhile')

describe('takeWhile', function()
  it('produces an error if its parent errors', function()
    createSingleUseOperator(
      "simulateError", 
      function (destination)
        destination:onError()
      end
    )
    local observable = Observable.of(''):simulateError()
    expect(observable).to.produce.error()
    expect(observable:takeWhile(function() end)).to.produce.error()
  end)

  it('uses the identity function if no predicate is specified', function()
    local observable = Observable.fromTable({true, true, false}):takeWhile()
    expect(observable).to.produce(true, true)
  end)

  it('stops producing values once the predicate returns false', function()
    local function isEven(x) return x % 2 == 0 end
    local observable = Observable.fromTable({2, 3, 4}, ipairs):takeWhile(isEven)
    expect(observable).to.produce(2)
  end)

  it('produces no values if the predicate never returns true', function()
    local function isEven(x) return x % 2 == 0 end
    local observable = Observable.fromTable({1, 3, 5}):takeWhile(isEven)
    expect(observable).to.produce.nothing()
  end)

  it('calls onError if the predicate errors', function()
    expect(Observable.fromRange(3):takeWhile(error)).to.produce.error()
  end)
end)
