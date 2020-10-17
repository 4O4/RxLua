local Observable = require("reactivex.observable")
local Observer = require("reactivex.observer")
local Subscription = require("reactivex.subscription")
local Subject = require("reactivex.subjects.subject")

require('reactivex.operators.catch')

describe('catch', function()
  it('ignores errors if no handler is specified', function()
    expect(Observable.throw():catch()).to.produce.nothing()
  end)

  it('continues producing values from the specified observable if the source errors', function()
    local handler = Subject.create()
    local observable = Observable.create(function(observer)
      observer:onNext(1)
      observer:onNext(2)
      observer:onError('ohno')
      observer:onNext(3)
      observer:onCompleted()
    end)

    handler:onNext(5)

    local onNext = observableSpy(observable:catch(handler))

    handler:onNext(6)
    handler:onNext(7)
    handler:onCompleted()

    expect(onNext).to.equal({{1}, {2}, {6}, {7}})
  end)

  it('allows a function as an argument', function()
    local handler = function() return Observable.empty() end
    expect(Observable.throw():catch(handler)).to.produce.nothing()
  end)

  it('calls onError if the supplied function errors', function()
    local handler = error
    expect(Observable.throw():catch(handler)).to.produce.error()
  end)

  it('calls onComplete when the parent completes', function()
    local onComplete = spy()
    Observable.throw():catch():subscribe(nil, nil, onComplete)
    expect(#onComplete).to.equal(1)
  end)
end)
