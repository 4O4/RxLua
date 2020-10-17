local Observer = require("reactivex.observer")
local Observable = require("reactivex.observable")
local Subject = require("reactivex.subjects.subject")
local Subscription = require("reactivex.subscription")

describe('Subject', function()
  describe('create', function()
    it('returns a Subject', function()
      expect(Subject.create()).to.be.an(Subject)
    end)
  end)

  describe('subscribe', function()
    it('returns a Subscription', function()
      local subject = Subject.create()
      local observer = Observer.create()
      expect(subject:subscribe(observer)).to.be.an(Subscription)
    end)

    it('accepts 3 functions as arguments', function()
      local onNext, onCompleted = spy(), spy()
      local subject = Subject.create()
      subject:subscribe(onNext, nil, onCompleted)
      subject:onNext(5)
      subject:onCompleted()
      expect(onNext).to.equal({{5}})
      expect(#onCompleted).to.equal(1)
    end)
  end)

  describe('onNext', function()
    it('pushes values to all subscribers', function()
      local observers = {}
      local spies = {}
      for i = 1, 2 do
        observers[i] = Observer.create()
        spies[i] = spy(observers[i], '_onNext')
      end

      local subject = Subject.create()
      subject:subscribe(observers[1])
      subject:subscribe(observers[2])
      subject:onNext(1)
      subject:onNext(2)
      subject:onNext(3)
      expect(spies[1]).to.equal({{1}, {2}, {3}})
      expect(spies[2]).to.equal({{1}, {2}, {3}})
    end)

    it('can be called using function syntax', function()
      local observer = Observer.create()
      local subject = Subject.create()
      local onNext = spy(observer, 'onNext')
      subject:subscribe(observer)
      subject(4)
      expect(#onNext).to.equal(1)
    end)
  end)

  describe('onError', function()
    it('pushes errors to all subscribers', function()
      local observers = {}
      local spies = {}
      for i = 1, 2 do
        observers[i] = Observer.create(nil, function() end, nil)
        spies[i] = spy(observers[i], '_onError')
      end

      local subject = Subject.create()
      subject:subscribe(observers[1])
      subject:subscribe(observers[2])
      subject:onError('ohno')
      expect(spies[1]).to.equal({{'ohno'}})
      expect(spies[2]).to.equal({{'ohno'}})
    end)
  end)

  describe('onCompleted', function()
    it('notifies all subscribers of completion', function()
      local observers = {}
      local spies = {}
      for i = 1, 2 do
        observers[i] = Observer.create(nil, function() end, nil)
        spies[i] = spy(observers[i], '_onCompleted')
      end

      local subject = Subject.create()
      subject:subscribe(observers[1])
      subject:subscribe(observers[2])
      subject:onCompleted()
      expect(#spies[1]).to.equal(1)
      expect(#spies[2]).to.equal(1)
    end)
  end)
end)
