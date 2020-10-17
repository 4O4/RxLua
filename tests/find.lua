local Observable = require("reactivex.observable")
local Observer = require("reactivex.observer")
local Subscription = require("reactivex.subscription")
local Subject = require("reactivex.subjects.subject")

require('reactivex.operators.find')

describe('find', function()
  it('produces an error if its parent errors', function()
    createSingleUseOperator(
      "simulateError", 
      function (destination)
        destination:onError()
      end
    )
    local observable = Observable.of(''):simulateError()
    expect(observable).to.produce.error()
    expect(observable:find()).to.produce.error()
  end)

  it('calls onError if the predicate errors', function()
    expect(Observable.of(3):find(error)).to.produce.error()
  end)

  it('uses the identity function as a predicate if none is specified', function()
    local observable = Observable.fromTable({false, false, true, true, false}):find()
    expect(observable).to.produce(true)
  end)

  it('passes all arguments to the predicate', function()
    local predicate = spy()

    local observable = Observable.create(function(observer)
      observer:onNext(1, 2, 3)
      observer:onNext(4, 5, 6)
      observer:onCompleted()
    end)

    observable:find(predicate):subscribe()

    expect(predicate).to.equal({{1, 2, 3}, {4, 5, 6}})
  end)

  it('produces the first element for which the predicate returns true and completes', function()
    local observable = Observable.fromRange(5):find(function(x) return x > 3 end)
    expect(observable).to.produce(4)
  end)

  it('completes after its parent completes if no value satisfied the predicate', function()
    local observable = Observable.fromRange(5):find(function() return false end)
    expect(observable).to.produce.nothing()
  end)
end)
