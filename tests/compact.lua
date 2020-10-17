local Observable = require("reactivex.observable")
local Observer = require("reactivex.observer")
local Subscription = require("reactivex.subscription")

require('reactivex.operators.compact')

describe('compact', function()
  it('produces an error if its parent errors', function()
    createSingleUseOperator(
      "simulateError", 
      function (destination)
        destination:onError()
      end
    )
    local observable = Observable.of(''):simulateError()
    expect(observable).to.produce.error()
    expect(observable:compact()).to.produce.error()
  end)

  it('does not produce values that are false or nil', function()
    local observable = Observable.create(function(observer)
      observer:onNext(nil)
      observer:onNext(true)
      observer:onNext(false)
      observer:onNext('')
      observer:onNext(0)
      observer:onCompleted()
    end)

    expect(observable:compact()).to.produce(true, '', 0)
  end)
end)
