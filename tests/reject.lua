local Observable = require("reactivex.observable")
local Observer = require("reactivex.observer")
local Subscription = require("reactivex.subscription")

require('reactivex.operators.reject')
require('reactivex.operators.unpack')

describe('reject', function()
  it('uses the identity function as the predicate if none is specified', function()
    local observable = Observable.fromTable({false, true}):reject()
    expect(observable).to.produce(false)
  end)

  it('passes all arguments to the predicate', function()
    local predicate = spy()
    Observable.fromTable({{1, 2}, {3, 4, 5}}, ipairs):unpack():reject(predicate):subscribe() -- FIXME: leaking coverage for unpack
    expect(predicate).to.equal({{1, 2}, {3, 4, 5}})
  end)

  it('does not produce elements that the predicate returns true for', function()
    local predicate = function(x) return x % 2 == 0 end
    local observable = Observable.fromRange(1, 5):reject(predicate)
    expect(observable).to.produce(1, 3, 5)
  end)

  it('errors when its parent errors', function()
    createSingleUseOperator(
      "simulateError", 
      function (destination)
        destination:onError()
      end
    )
    local observable = Observable.of(''):simulateError()
    expect(observable).to.produce.error()
    expect(observable:reject()).to.produce.error()
  end)

  it('calls onError when the predicate errors', function()
    expect(Observable.fromRange(3):reject(error)).to.produce.error()
  end)
end)
