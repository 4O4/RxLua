local Observable = require("reactivex.observable")
local Observer = require("reactivex.observer")
local Subscription = require("reactivex.subscription")
local Subject = require("reactivex.subjects.subject")

require('reactivex.operators.flatMapLatest')

describe('flatMapLatest', function()
  it('produces an error if its parent errors', function()
    expect(Observable.throw():flatMapLatest()).to.fail()
  end)

  it('produces an error if the callback errors', function()
    expect(Observable.fromRange(3):flatMapLatest(error)).to.produce.error()
  end)

  it('unsubscribes from the source and the projected observable', function()
    local outerUnsubscribe = spy()
    local outerSubscription = Subscription.create(outerUnsubscribe)
    local outer = Observable.create(function(observer)
      observer:onNext()
      observer:onCompleted()
      return outerSubscription
    end)

    local innerUnsubscribe = spy()
    local innerSubscription = Subscription.create(innerUnsubscribe)
    local inner = Observable.create(function()
      return innerSubscription
    end)

    local subscription = outer:flatMapLatest(function() return inner end):subscribe()
    subscription:unsubscribe()
    expect(#innerUnsubscribe).to.equal(1)
    expect(#outerUnsubscribe).to.equal(1)
  end)

  it('uses the identity function as the callback if none is specified', function()
    local observable = Observable.fromTable({
      Observable.fromRange(3),
      Observable.fromRange(5)
    }):flatMapLatest()
    expect(observable).to.produce(1, 2, 3, 1, 2, 3, 4, 5)
  end)

  it('produces values from the most recent projected Observable of the source', function()
    local children = {Subject.create(), Subject.create()}
    local subject = Subject.create()
    local onNext = observableSpy(subject:flatMapLatest(function(i)
      return children[i]
    end))
    subject:onNext(1)
    children[1]:onNext(1)
    children[1]:onNext(2)
    children[1]:onNext(3)
    children[2]:onNext(10)
    subject:onNext(2)
    children[1]:onNext(4)
    children[2]:onNext(20)
    children[1]:onNext(5)
    children[2]:onNext(30)
    children[2]:onNext(40)
    children[2]:onNext(50)
    expect(onNext).to.equal({{1}, {2}, {3}, {20}, {30}, {40}, {50}})
  end)

  it('does not complete if one of the children completes', function()
    local subject = Subject.create()
    local flatMapped = subject:flatMapLatest(function() return Observable.empty() end)
    local _, _, onCompleted = observableSpy(flatMapped)
    subject:onNext()
    expect(#onCompleted).to.equal(0)
    subject:onNext()
    expect(#onCompleted).to.equal(0)
    subject:onCompleted()
    expect(#onCompleted).to.equal(1)
  end)
end)
