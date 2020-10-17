local Observable = require("reactivex.observable")
local Subscription = require("reactivex.subscription")
local CooperativeScheduler = require("reactivex.schedulers.cooperativescheduler")

local Observable = require("reactivex.observable")
local Observer = require("reactivex.observer")
local Subscription = require("reactivex.subscription")

describe('Observable', function()
  describe('create', function()
    local function createObservable(...)
      return
    end

    it('works when no subscribe function is passed', function()
      local observable
      expect(function () observable = Observable.create() end).to_not.fail()
      expect(observable).to.be.an(Observable)
    end)

    it('works when a subscribe function is passed', function()
      local observable
      expect(function () observable = Observable.create(function () end) end).to_not.fail()
      expect(observable).to.be.an(Observable)
    end)
  end)

  describe('subscribe', function()
    it('creates a subscription', function ()
      local observable = Observable.create(function () end)
      local sub = observable:subscribe()
      expect(observable).to.be.an(Observable)
    end)

    it('uses the `subscribe` function if it was provided', function()
      local subscriptionLogic = spy()
      local observable = Observable.create(subscriptionLogic)
      observable:subscribe()
      expect(#subscriptionLogic).to.equal(1)
    end)
  end)

  describe('empty', function()
    it('returns an Observable that does not produce any values', function()
      local observable = Observable.empty()
      local onNext, onError, onCompleted = observableSpy(observable)
      expect(#onNext).to.equal(0)
      expect(#onError).to.equal(0)
      expect(#onCompleted).to.equal(1)
    end)
  end)

  describe('never', function()
    it('returns an Observable that does not produce values or complete', function()
      local observable = Observable.never()
      local onNext, onError, onCompleted = observableSpy(observable)
      expect(#onNext).to.equal(0)
      expect(#onError).to.equal(0)
      expect(#onCompleted).to.equal(0)
    end)
  end)

  describe('throw', function()
    it('returns an Observable that produces an error', function()
      local observable = Observable.throw('message')
      expect(observable).to.produce.error()
    end)
  end)

  describe('of', function()
    it('returns an Observable that produces the supplied arguments and completes', function()
      local observable = Observable.of(1, 2, 3)
      expect(observable).to.produce(1, 2, 3)
    end)

    it('returns an Observable that produces nil and completes if nil is passed', function()
      local observable = Observable.of(nil)
      expect(observable).to.produce(nil)
    end)

    it('returns an Observable that produces nothing if no arguments are passed', function()
      local observable = Observable.of()
      expect(observable).to.produce.nothing()
    end)
  end)

  describe('fromRange', function()
    it('errors if no arguments are provided', function()
      local run = function() Observable.fromRange():subscribe() end
      expect(run).to.fail()
    end)

    describe('with one argument', function()
      it('returns an Observable that produces elements sequentially from 1 to the first argument', function()
        local observable = Observable.fromRange(5)
        expect(observable).to.produce(1, 2, 3, 4, 5)
      end)

      it('returns an Observable that produces no elements if the first argument is less than one', function()
        local observable = Observable.fromRange(0)
        expect(observable).to.produce.nothing()
      end)
    end)

    describe('with two arguments', function()
      it('returns an Observable that produces elements sequentially from the first argument to the second argument', function()
        local observable = Observable.fromRange(1, 5)
        expect(observable).to.produce(1, 2, 3, 4, 5)
      end)

      it('returns an Observable that produces no elements if the first argument is greater than the second argument', function()
        local observable = Observable.fromRange(1, -5)
        expect(observable).to.produce.nothing()
      end)
    end)

    describe('with three arguments', function()
      it('returns an Observable that produces elements sequentially from the first argument to the second argument, incrementing by the third argument', function()
        local observable = Observable.fromRange(1, 5, 2)
        expect(observable).to.produce(1, 3, 5)
      end)
    end)
  end)

  describe('fromTable', function()
    it('errors if the first argument is not a table', function()
      local function run() Observable.fromTable():subscribe() end
      expect(run).to.fail()
    end)

    describe('with one argument', function()
      it('returns an Observable that produces values by iterating the table using pairs', function()
        local input = {foo = 'bar', 1, 2, 3}
        local observable = Observable.fromTable(input)
        local result = {}
        for key, value in pairs(input) do table.insert(result, {value}) end
        expect(observable).to.produce(result)
      end)
    end)

    describe('with two arguments', function()
      it('returns an Observable that produces values by iterating the table using the second argument', function()
        local input = {foo = 'bar', 3, 4, 5}
        local observable = Observable.fromTable(input, ipairs)
        expect(observable).to.produce(3, 4, 5)
      end)
    end)

    describe('with three arguments', function()
      it('returns an Observable that produces value-key pairs by iterating the table if the third argument is true', function()
        local input = {foo = 'bar', 3, 4, 5}
        local observable = Observable.fromTable(input, ipairs, true)
        expect(observable).to.produce({{3, 1}, {4, 2}, {5, 3}})
      end)
    end)
  end)

  describe('fromCoroutine', function()
    it('returns an Observable that produces a value whenever the first argument yields a value', function()
      local coroutine = coroutine.create(function()
        coroutine.yield(1)
        coroutine.yield(2)
        return 3
      end)

      local scheduler = CooperativeScheduler.create()
      local observable = Observable.fromCoroutine(coroutine, scheduler)
      local onNext, onError, onCompleted = observableSpy(observable)
      repeat scheduler:update()
      until scheduler:isEmpty()
      expect(onNext).to.equal({{1}, {2}, {3}})
    end)

    it('accepts a function as the first argument and wraps it into a coroutine', function()
      local coroutine = function()
        coroutine.yield(1)
        coroutine.yield(2)
        return 3
      end

      local scheduler = CooperativeScheduler.create()
      local observable = Observable.fromCoroutine(coroutine, scheduler)
      local onNext, onError, onCompleted = observableSpy(observable)
      repeat scheduler:update()
      until scheduler:isEmpty()
      expect(onNext).to.equal({{1}, {2}, {3}})
    end)

    it('shares values among Observers when the first argument is a coroutine', function()
      local coroutine = coroutine.create(function()
        coroutine.yield(1)
        coroutine.yield(2)
        return 3
      end)

      local scheduler = CooperativeScheduler.create()
      local observable = Observable.fromCoroutine(coroutine, scheduler)
      local onNextA = observableSpy(observable)
      local onNextB = observableSpy(observable)

      repeat scheduler:update()
      until scheduler:isEmpty()

      expect(onNextA).to.equal({{1}, {3}})
      expect(onNextB).to.equal({{2}})
    end)

    it('uses a unique coroutine for each Observer when the first argument is a function', function()
      local coroutine = function()
        coroutine.yield(1)
        coroutine.yield(2)
        return 3
      end

      local scheduler = CooperativeScheduler.create()
      local observable = Observable.fromCoroutine(coroutine, scheduler)
      local onNextA, onErrorA, onCompletedA = observableSpy(observable)
      local onNextB, onErrorB, onCompletedB = observableSpy(observable)
      local onNextC, onErrorC, onCompletedC = observableSpy(observable)
      repeat scheduler:update()
      until scheduler:isEmpty()

      expect(onNextA).to.equal({{1}, {2}, {3}})
      expect(onNextB).to.equal({{1}, {2}, {3}})
      expect(onNextC).to.equal({{1}, {2}, {3}})
      expect(#onCompletedA).to.equal(1)
      expect(#onCompletedB).to.equal(1)
      expect(#onCompletedC).to.equal(1)
    end)
  end)

  describe('fromFileByLine', function()
    local oldIO = _G['io']
    _G['io'] = {}

    local filename = 'file.txt'

    it('returns an observable', function()
      expect(Observable.fromFileByLine(filename)).to.be.an(Observable)
    end)

    it('errors if the file does not exist', function()
      io.open = function() return nil end
      local onError = spy()
      Observable.fromFileByLine(filename):subscribe(nil, onError, nil)
      expect(onError).to.equal({{ filename }})
    end)

    it('returns an Observable that produces the lines of the file', function()
      io.open = function() return { close = function() end } end
      io.lines = function()
        local lines = { 'line1', 'line2', 'line3' }
        local i = 0
        return function()
          i = i + 1
          return lines[i]
        end
      end

      expect(Observable.fromFileByLine(filename)).to.produce('line1', 'line2', 'line3')
    end)

    io = oldIO
  end)

  describe('defer', function()
    it('returns an Observable', function()
      expect(Observable.defer(function() end)).to.be.an(Observable)
    end)

    it('fails if no factory is specified', function()
      expect(function () Observable.defer() end).to.fail()
    end)

    it('fails if the factory does not return an Observable', function()
      expect(function () Observable.defer(function() end):subscribe() end).to.fail()
    end)

    it('uses the factory function to create a new Observable for each subscriber', function()
      local i = 0
      local function factory()
        i = i + 1
        return Observable.fromRange(i, 3)
      end
      expect(Observable.defer(factory)).to.produce(1, 2, 3)
      expect(Observable.defer(factory)).to.produce(2, 3)
      expect(Observable.defer(factory)).to.produce(3)
    end)

    it('returns Observables that return subscriptions from their subscribe function', function()
      local function factory()
        return Observable.create(function()
          return Subscription.create()
        end)
      end

      expect(Observable.defer(factory):subscribe()).to.be.a(Subscription)

      local function factory2()
        return Observable.create(function()
          return function () end
        end)
      end

      expect(Observable.defer(factory2):subscribe()).to.be.a(Subscription)
    end)
  end)

  describe('replicate', function()
    it('returns an Observable', function()
      expect(Observable.replicate()).to.be.an(Observable)
    end)

    it('returns an Observable that produces the first argument a specified number of times', function()
      expect(Observable.replicate(1, 3)).to.produce(1, 1, 1)
    end)

    it('produces nothing if the count is less than or equal to zero', function()
      expect(Observable.replicate(1, 0)).to.produce.nothing()
      expect(Observable.replicate(1, -1)).to.produce.nothing()
    end)
  end)

  describe('dump', function()
  end)

  describe('automatically unsubscribes on error', function ()
    describe('when error is emitted during execution of `subscribe` logic', function ()
      it('and subscribe function returns plain teardown function', function ()
        local teardownSpy = spy()
        local observable = Observable.create(function (observer)
          observer:onError()
          return teardownSpy
        end)

        observable:subscribe()
        expect(#teardownSpy).to.equal(1)
      end)

      it('and subscribe function returns Subscription', function ()
        local teardownSpy = spy()
        local observable = Observable.create(function (observer)
          observer:onError()
          return Subscription.create(function ()
            teardownSpy()
          end)
        end)

        observable:subscribe()
        expect(#teardownSpy).to.equal(1)
      end)
    end)

    describe('when error is emitted later after subscription logic has been fully executed', function ()
      it('and subscribe function returns plain teardown function', function ()
        local teardownSpy = spy()
        local observer

        local observable = Observable.create(function (o)
          observer = o
          return teardownSpy
        end)

        observable:subscribe()
        observer:onError()
        expect(#teardownSpy).to.equal(1)
      end)

      it('and subscribe function returns Subscription', function ()
        local teardownSpy = spy()
        local observer

        local observable = Observable.create(function (o)
          observer = o
          return Subscription.create(function ()
            teardownSpy()
          end)
        end)

        observable:subscribe()
        observer:onError()
        expect(#teardownSpy).to.equal(1)
      end)
    end)
  end)

  describe('automatically unsubscribes on completion', function ()
    describe('when completion is emitted during execution of `subscribe` logic', function ()
      it('and subscribe function returns plain teardown function', function ()
        local teardownSpy = spy()
        local observable = Observable.create(function (observer)
          observer:onCompleted()
          return teardownSpy
        end)
        observable:subscribe()
        expect(#teardownSpy).to.equal(1)
      end)

      it('and subscribe function returns Subscription', function ()
        local teardownSpy = spy()
        local observable = Observable.create(function (observer)
          observer:onCompleted()
          return Subscription.create(function ()
            teardownSpy()
          end)
        end)
        observable:subscribe()
        expect(#teardownSpy).to.equal(1)
      end)
    end)

    describe('when completion is emitted later after subscription logic has been fully executed', function ()
      it('and subscribe function returns plain teardown function', function ()
        local teardownSpy = spy()
        local observer

        local observable = Observable.create(function (o)
          observer = o
          return teardownSpy
        end)

        observable:subscribe()
        observer:onCompleted()
        expect(#teardownSpy).to.equal(1)
      end)

      it('and subscribe function returns a Subscription', function ()
        local teardownSpy = spy()
        local observer

        local observable = Observable.create(function (o)
          observer = o
          return Subscription.create(function ()
            teardownSpy()
          end)
        end)

        observable:subscribe()
        observer:onCompleted()
        expect(#teardownSpy).to.equal(1)
      end)
    end)
  end)
end)
