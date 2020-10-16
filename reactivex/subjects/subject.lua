local Observable = require 'reactivex.observable'
local Observer = require 'reactivex.observer'
local Subscription = require 'reactivex.subscription'
local util = require 'reactivex.util'
local AnonymousSubject = require 'reactivex.subjects.anonymoussubject'
local SubjectSubscription = require 'reactivex.subjectsubscription'

--- @class Subject
-- @description Subjects function both as an Observer and as an Observable. Subjects inherit all
-- Observable functions, including subscribe. Values can also be pushed to the Subject, which will
-- be broadcasted to any subscribed Observers.
local Subject = setmetatable({}, Observable)
Subject.__index = Subject
Subject.__tostring = util.constant('Subject')
table.insert(Subject.___isa, Subject)

--- Creates a new Subject.
-- @returns {Subject}
function Subject.create()
  local baseObservable = Observable.create()
  local self = setmetatable(baseObservable, Subject)
  self.observers = {}
  self.stopped = false
  self._unsubscribed = false

  return self
end

-- Creates a new Subject, with this Subject as the source. It must be used internally by operators to create a proper chain of observables.
-- @arg {function} createObserver - observer factory function
-- @returns {Subject} - a new Subject chained with the source Subject
function Subject:lift(createObserver)
  return AnonymousSubject.create(self, createObserver)
end

local DummyEntryForDocs = {}
--- Creates a new Observer or uses the exxisting one, and registers Observer handlers for notifications the Subject will emit.
-- @arg {function|Observer} onNext|observer - Called when the Observable produces a value.
-- @arg {function} onError - Called when the Observable terminates due to an error.
-- @arg {function} onCompleted - Called when the Observable completes normally.
-- @returns {Subscription} a Subscription object which you can call `unsubscribe` on to stop all work that the Observable does.
function DummyEntryForDocs:subscribe(onNext, onError, onCompleted) end

function Subject:_subscribe(observer)
  if self._unsubscribed then
    error('Object is unsubscribed')
  elseif self.hasError then
    observer:onError(self.thrownError)
    return Subscription.EMPTY
  elseif self.stopped then
    observer:onCompleted()
    return Subscription.EMPTY
  else
    table.insert(self.observers, observer)
    return SubjectSubscription.create(self, observer)
  end
end

--- Pushes zero or more values to the Subject. They will be broadcasted to all Observers.
-- @arg {*...} values values to the Subject. They will be broadcasted to all Observers.
---@param values *...
function Subject:onNext(...)
  if self._unsubscribed then
    error('Object is unsubscribed')
  end

  if not self.stopped then
    local observers = { util.unpack(self.observers) }

    for i = 1, #observers do
      observers[i]:onNext(...)
    end
  end
end

--- Signal to all Observers that an error has occurred.
-- @arg {string=} message - A string describing what went wrong.
function Subject:onError(message)
  if self._unsubscribed then
    error('Object is unsubscribed')
  end

  if not self.stopped then
    self.stopped = true

    for i = #self.observers, 1, -1 do
      self.observers[i]:onError(message)
    end

    self.observers = {}
  end
end

--- Signal to all Observers that the Subject will not produce any more values.
function Subject:onCompleted()
  if self._unsubscribed then
    error('Object is unsubscribed')
  end

  if not self.stopped then
    self.stopped = true

    for i = #self.observers, 1, -1 do
      self.observers[i]:onCompleted()
    end

    self.observers = {}
  end
end

Subject.__call = Subject.onNext

return Subject
