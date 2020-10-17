local Observable = require("reactivex.observable")
local Observer = require("reactivex.observer")
local Subscription = require("reactivex.subscription")

require('reactivex.operators.skipWhile')

describe('skipWhile', function()
  it('produces an error if its parent errors', function()
    createSingleUseOperator(
      "simulateError", 
      function (destination)
        destination:onError()
      end
    )
    local observable = Observable.of(''):simulateError()
    expect(observable).to.produce.error()
    expect(observable:skipWhile(function() end)).to.produce.error()
  end)

  it('uses the identity function if no predicate is specified', function()
    local observable = Observable.fromTable({true, true, false}):skipWhile()
    expect(observable).to.produce(false)
  end)

  it('produces values once the predicate returns false', function()
    local function isEven(x) return x % 2 == 0 end
    local observable = Observable.fromTable({2, 3, 4}, ipairs):skipWhile(isEven)
    expect(observable).to.produce(3, 4)
  end)

  it('produces no values if the predicate never returns false', function()
    local function isEven(x) return x % 2 == 0 end
    local observable = Observable.fromTable({2, 4, 6}):skipWhile(isEven)
    expect(observable).to.produce.nothing()
  end)

  it('calls onError if the predicate errors', function()
    expect(Observable.fromRange(3):skipWhile(error)).to.produce.error()
  end)
end)
