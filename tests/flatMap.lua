local Observable = require("reactivex.observable")
local Observer = require("reactivex.observer")
local Subscription = require("reactivex.subscription")

require('reactivex.operators.flatMap')

describe('flatMap', function()
  it('produces an error if its parent errors', function()
    createSingleUseOperator(
      "simulateError", 
      function (destination)
        destination:onError()
      end
    )
    local observable = Observable.of(''):simulateError()
    expect(observable).to.produce.error()
    expect(observable:flatMap()).to.produce.error()
  end)

  it('uses the identity function as the callback if none is specified', function()
    local observable = Observable.fromTable{
      Observable.fromRange(3),
      Observable.fromRange(5)
    }:flatMap()
    expect(observable).to.produce(1, 2, 3, 1, 2, 3, 4, 5)
  end)

  it('produces all values produced by the observables produced by its parent', function()
    local observable = Observable.fromRange(3):flatMap(function(i)
      return Observable.fromRange(i, 3)
    end)

    expect(observable).to.produce(1, 2, 3, 2, 3, 3)
  end)

  -- FIXME: Does this test make any sense? Why is it using delay? Why the coop scheduler?
  --        Why doesn't delay and coop scheduler have their own tests?
  -- it('completes after all observables produced by its parent', function()
  --   local scheduler = CooperativeScheduler.create()
  --   local observable = Observable.fromRange(3):flatMap(function(i)
  --     return Observable.fromRange(i, 3):delay(i, scheduler)
  --   end)

  --   local onNext, onError, onCompleted, order = observableSpy(observable)
  --   repeat scheduler:update(1)
  --   until scheduler:isEmpty()
  --   expect(#onNext).to.equal(6)
  --   expect(#onCompleted).to.equal(1)
  -- end)
end)
