local util = require 'reactivex.util'
local Subscription = require 'reactivex.subscription'

--- @class Observer
-- @description Observers are simple objects that receive values from Observables.
local Observer = setmetatable({}, Subscription)
Observer.__index = Observer
Observer.__tostring = util.constant('Observer')

--- Creates a new Observer.
-- @arg {function=} onNext - Called when the Observable produces a value.
-- @arg {function=} onError - Called when the Observable terminates due to an error.
-- @arg {function=} onCompleted - Called when the Observable completes normally.
-- @returns {Observer}
function Observer.create(...)
  local args = {...}
  local argsCount = select('#', ...)
  local destinationOrNext, onError, onCompleted = args[1], args[2], args[3]
  local self = setmetatable(Subscription.create(), Observer)
  self.stopped = false
  self._onNext = Observer.EMPTY._onNext
  self._onError = Observer.EMPTY._onError
  self._onCompleted = Observer.EMPTY._onCompleted
  self._rawCallbacks = {}

  if argsCount > 0 then
    if util.isa(destinationOrNext, Observer) then
      self._onNext = destinationOrNext._onNext
      self._onError = destinationOrNext._onError
      self._onCompleted = destinationOrNext._onCompleted
      self._rawCallbacks = destinationOrNext._rawCallbacks
    else
      self._rawCallbacks.onNext = destinationOrNext
      self._rawCallbacks.onError = onError
      self._rawCallbacks.onCompleted = onCompleted

      self._onNext = function (...)
        if self._rawCallbacks.onNext then
          self._rawCallbacks.onNext(...)
        end
      end
      self._onError = function (...)
        if self._rawCallbacks.onError then
          self._rawCallbacks.onError(...)
        end
      end
      self._onCompleted = function ()
        if self._rawCallbacks.onCompleted then
          self._rawCallbacks.onCompleted()
        end
      end
    end
  end

  return self
end

--- Pushes zero or more values to the Observer.
-- @arg {*...} values
function Observer:onNext(...)
  if not self.stopped then
    self._onNext(...)
  end
end

--- Notify the Observer that an error has occurred.
-- @arg {string=} message - A string describing what went wrong.
function Observer:onError(message)
  if not self.stopped then
    self.stopped = true
    self._onError(message)
    self:unsubscribe()
  end
end

--- Notify the Observer that the sequence has completed and will produce no more values.
function Observer:onCompleted()
  if not self.stopped then
    self.stopped = true
    self._onCompleted()
    self:unsubscribe()
  end
end

function Observer:unsubscribe()
  if self._unsubscribed then
    return
  end

  self.stopped = true
  Subscription.unsubscribe(self)
end

Observer.EMPTY = {
  _unsubscribed = true,
  _onNext = util.noop,
  _onError = error,
  _onCompleted = util.noop,
}

return Observer
