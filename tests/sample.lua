local Observable = require("reactivex.observable")
local Observer = require("reactivex.observer")
local Subscription = require("reactivex.subscription")
local Subject = require("reactivex.subjects.subject")

require('reactivex.operators.sample')

describe('sample', function()
  it('errors if no sampler is specified', function()
    expect(function() Observable.empty():sample() end).to.fail()
  end)

  it('produces nil values if the sampler fires before the source does', function()
    local sampler = Observable.fromRange(3)
    local onNext, onError, onCompleted = observableSpy(Observable.empty():sample(sampler))
    expect(#onNext).to.equal(0)
  end)

  it('produces the latest value produced by the source when the sampler fires', function()
    local a = Subject.create()
    local b = Subject.create()
    local onNext, onError, onCompleted = observableSpy(a:sample(b))
    a:onNext(1)
    b:onNext('a')
    b:onNext('b')
    a:onNext(2)
    a:onNext(3)
    b:onNext('c')
    a:onCompleted()
    b:onCompleted()
    expect(onNext).to.equal({{1}, {1}, {3}})
  end)

  it('completes when the sampler completes', function()
    local a = Subject.create()
    local b = Subject.create()
    local onNext, onError, onCompleted = observableSpy(a:sample(b))
    a:onNext(1)
    -- a:onCompleted() -- you loose your source if you do it, doesn't make sense
    b:onNext('a')
    b:onNext('b')
    b:onNext('c')
    expect(#onCompleted).to.equal(0)
    b:onCompleted()
    expect(#onCompleted).to.equal(1)
    expect(onNext).to.equal({{1}, {1}, {1}})
  end)

  it('errors when the source errors', function()
    local a = Observable.throw()
    local b = Observable.never()
    expect(a:sample(b)).to.produce.error()
  end)

  it('errors when the sampler errors', function()
    local a = Observable.never()
    local b = Observable.throw()
    expect(a:sample(b)).to.produce.error()
  end)
end)
