local util = require 'reactivex.util'
local Observer = require 'reactivex.observer'

--- @class Observable
-- @description Observables push values to Observers.
local Observable = {}
Observable.__index = Observable
Observable.__tostring = util.constant('Observable')
Observable.___isa = { Observable }

--- Creates a new Observable. Please not that the Observable does not do any work right after creation, but only after calling a `subscribe` on it.
-- @arg {function} subscribe - The subscription function that produces values. It is called when the Observable 
--                             is initially subscribed to. This function is given an Observer, to which new values
--                             can be `onNext`ed, or an `onError` method can be called to raise an error, or `onCompleted`
--                             can be called to notify of a successful completion.
-- @returns {Observable}
function Observable.create(subscribe)
  local self = {}
  local subscribe = subscribe

  if subscribe then
    self._subscribe = function (self, ...) return subscribe(...) end
  end

  return setmetatable(self, Observable)
end

-- Creates a new Observable, with this Observable as the source. It must be used internally by operators to create a proper chain of observables.
-- @arg {function} createObserver observer factory function
-- @returns {Observable} a new observable chained with the source observable
function Observable:lift(createObserver)
  local this = self
  local createObserver = createObserver

  return Observable.create(function (observer)
    return this:subscribe(createObserver(observer))
  end)
end

--- Invokes an execution of an Observable and registers Observer handlers for notifications it will emit.
-- @arg {function|Observer} onNext|observer - Called when the Observable produces a value.
-- @arg {function} onError - Called when the Observable terminates due to an error.
-- @arg {function} onCompleted - Called when the Observable completes normally.
-- @returns {Subscription} a Subscription object which you can call `unsubscribe` on to stop all work that the Observable does.
function Observable:subscribe(observerOrNext, onError, onCompleted)
  local sink

  if util.isa(observerOrNext, Observer) then
    sink = observerOrNext
  else
    sink = Observer.create(observerOrNext, onError, onCompleted)
  end

  sink:add(self:_subscribe(sink))

  return sink
end

--- Returns an Observable that immediately completes without producing a value.
function Observable.empty()
  return Observable.create(function(observer)
    observer:onCompleted()
  end)
end

--- Returns an Observable that never produces values and never completes.
function Observable.never()
  return Observable.create(function(observer) end)
end

--- Returns an Observable that immediately produces an error.
function Observable.throw(message)
  return Observable.create(function(observer)
    observer:onError(message)
  end)
end

--- Creates an Observable that produces a set of values.
-- @arg {*...} values
-- @returns {Observable}
function Observable.of(...)
  local args = {...}
  local argCount = select('#', ...)
  return Observable.create(function(observer)
    for i = 1, argCount do
      observer:onNext(args[i])
    end

    observer:onCompleted()
  end)
end

--- Creates an Observable that produces a range of values in a manner similar to a Lua for loop.
-- @arg {number} initial - The first value of the range, or the upper limit if no other arguments
--                         are specified.
-- @arg {number=} limit - The second value of the range.
-- @arg {number=1} step - An amount to increment the value by each iteration.
-- @returns {Observable}
function Observable.fromRange(initial, limit, step)
  if not limit and not step then
    initial, limit = 1, initial
  end

  step = step or 1

  return Observable.create(function(observer)
    for i = initial, limit, step do
      observer:onNext(i)
    end

    observer:onCompleted()
  end)
end

--- Creates an Observable that produces values from a table.
-- @arg {table} table - The table used to create the Observable.
-- @arg {function=pairs} iterator - An iterator used to iterate the table, e.g. pairs or ipairs.
-- @arg {boolean} keys - Whether or not to also emit the keys of the table.
-- @returns {Observable}
function Observable.fromTable(t, iterator, keys)
  iterator = iterator or pairs
  return Observable.create(function(observer)
    for key, value in iterator(t) do
      observer:onNext(value, keys and key or nil)
    end

    observer:onCompleted()
  end)
end

--- Creates an Observable that produces values when the specified coroutine yields.
-- @arg {thread|function} fn - A coroutine or function to use to generate values.  Note that if a
--                             coroutine is used, the values it yields will be shared by all
--                             subscribed Observers (influenced by the Scheduler), whereas a new
--                             coroutine will be created for each Observer when a function is used.
-- @returns {Observable}
function Observable.fromCoroutine(fn, scheduler)
  return Observable.create(function(observer)
    local thread = type(fn) == 'function' and coroutine.create(fn) or fn
    return scheduler:schedule(function()
      while not observer.stopped do
        local success, value = coroutine.resume(thread)

        if success then
          observer:onNext(value)
        else
          return observer:onError(value)
        end

        if coroutine.status(thread) == 'dead' then
          return observer:onCompleted()
        end

        coroutine.yield()
      end
    end)
  end)
end

--- Creates an Observable that produces values from a file, line by line.
-- @arg {string} filename - The name of the file used to create the Observable
-- @returns {Observable}
function Observable.fromFileByLine(filename)
  return Observable.create(function(observer)
    local file = io.open(filename, 'r')
    if file then
      file:close()

      for line in io.lines(filename) do
        observer:onNext(line)
      end

      return observer:onCompleted()
    else
      return observer:onError(filename)
    end
  end)
end

--- Creates an Observable that creates a new Observable for each observer using a factory function.
-- @arg {function} factory - A function that returns an Observable.
-- @returns {Observable}
function Observable.defer(fn)
  if not fn or type(fn) ~= 'function' then
    error('Expected a function')
  end

  return setmetatable({
    subscribe = function(_, ...)
      local observable = fn()
      return observable:subscribe(...)
    end
  }, Observable)
end

--- Returns an Observable that repeats a value a specified number of times.
-- @arg {*} value - The value to repeat.
-- @arg {number=} count - The number of times to repeat the value.  If left unspecified, the value
--                        is repeated an infinite number of times.
-- @returns {Observable}
function Observable.replicate(value, count)
  return Observable.create(function(observer)
    while count == nil or count > 0 do
      observer:onNext(value)
      if count then
        count = count - 1
      end
    end
    observer:onCompleted()
  end)
end

--- Subscribes to this Observable and prints values it produces.
-- @arg {string=} name - Prefixes the printed messages with a name.
-- @arg {function=tostring} formatter - A function that formats one or more values to be printed.
function Observable:dump(name, formatter)
  name = name and (name .. ' ') or ''
  formatter = formatter or tostring

  local onNext = function(...) print(name .. 'onNext: ' .. formatter(...)) end
  local onError = function(e) print(name .. 'onError: ' .. e) end
  local onCompleted = function() print(name .. 'onCompleted') end

  return self:subscribe(onNext, onError, onCompleted)
end

return Observable
