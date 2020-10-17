local Observable = require("reactivex.observable")
local Observer = require("reactivex.observer")
local Subscription = require("reactivex.subscription")

require('reactivex.operators.take')

describe('take', function()
  it('produces an error if its parent errors', function()
    createSingleUseOperator(
      "simulateError", 
      function (destination)
        destination:onError()
      end
    )
    local observable = Observable.of(''):simulateError()
    expect(observable).to.produce.error()
    expect(observable:take(1)).to.produce.error()
  end)

  it('produces nothing if the count is zero', function()
    local observable = Observable.of(3):take(0)
    expect(observable).to.produce.nothing()
  end)

  it('produces nothing if the count is less than zero', function()
    local observable = Observable.of(3):take(-3)
    expect(observable).to.produce.nothing()
  end)

  it('takes one element if no count is specified', function()
    local observable = Observable.fromTable({2, 3, 4}, ipairs):take()
    expect(observable).to.produce(2)
  end)

  it('produces all values if it takes all of the values of the original', function()
    local observable = Observable.fromTable({1, 2}, ipairs):take(2)
    expect(observable).to.produce(1, 2)
  end)

  it('completes and does not fail if it takes more values than were produced', function()
    local observable = Observable.of(3):take(5)
    local onNext, onError, onCompleted = observableSpy(observable)
    expect(onNext).to.equal({{3}})
    expect(#onError).to.equal(0)
    expect(#onCompleted).to.equal(1)
  end)

  describe('guarantees that source does not emit anything more after all values have been taken', function ()
    local function applyDummyOperator(source, fn, asd)
      return Observable.create(function(observer)
        local function onNext(...)
          fn()
          return observer:onNext(...)
        end
    
        local function onError(e)
          return observer:onError(e)
        end
    
        local function onCompleted()
          return observer:onCompleted()
        end

        return  source:subscribe(onNext, onError, onCompleted)
      end)
    end

    it('when take() is the last operator in the chain', function ()
      local observer
      local preTakeOperator1Spy, preTakeOperator2Spy = spy(), spy()
      local source = Observable.create(function (o) observer = o end)

      local observable = applyDummyOperator(source, preTakeOperator1Spy, "aaa")
      observable = applyDummyOperator(observable, preTakeOperator2Spy, "bbb")
      observable = observable:take(2)
  
      observable:subscribe(function () end)
  
      observer:onNext(1)
      observer:onNext(2)
      observer:onNext(3)
      observer:onNext(4)
      observer:onNext(5)

      expect(#preTakeOperator1Spy).to.equal(2)
      expect(#preTakeOperator2Spy).to.equal(2)
    end)

    it('when take() is not the last operator in the chain', function ()
      local observer
      local preTakeOperator1Spy, preTakeOperator2Spy, postTakeOperator1Spy = spy(), spy(), spy()
      local source = Observable.create(function (o) observer = o end)

      local observable = applyDummyOperator(source, preTakeOperator1Spy)
      observable = applyDummyOperator(observable, preTakeOperator2Spy)
      observable = observable:take(2)
      observable = applyDummyOperator(observable, postTakeOperator1Spy)
  
      observable:subscribe(function () end)
  
      observer:onNext(1)
      observer:onNext(2)
      observer:onNext(3)
      observer:onNext(4)
      observer:onNext(5)

      expect(#preTakeOperator1Spy).to.equal(2)
      expect(#preTakeOperator2Spy).to.equal(2)
      expect(#postTakeOperator1Spy).to.equal(2)
    end)
  end)
end)
