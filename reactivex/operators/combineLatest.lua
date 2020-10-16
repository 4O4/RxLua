local Observable = require 'reactivex.observable'
local Subscription = require 'reactivex.subscription'
local util = require 'reactivex.util'
local Observer = require 'reactivex.observer'

--- Returns a new Observable that runs a combinator function on the most recent values from a set
-- of Observables whenever any of them produce a new value. The results of the combinator function
-- are produced by the new Observable.
-- @arg {Observable...} observables - One or more Observables to combine.
-- @arg {function} combinator - A function that combines the latest result from each Observable and
--                              returns a single value.
-- @returns {Observable}
function Observable:combineLatest(...)
  local sources = {...}
  local combinator = table.remove(sources)
  if not util.isCallable(combinator) then
    table.insert(sources, combinator)
    combinator = function(...) return ... end
  end
  table.insert(sources, 1, self)

  return self:lift(function (destination)
    local latest = {}
    local pending = {util.unpack(sources)}
    local completedCount = 0

    local function createOnNext(i)
      return function(value)
        latest[i] = value
        pending[i] = nil

        if not next(pending) then
          util.tryWithObserver(destination, function()
            destination:onNext(combinator(util.unpack(latest)))
          end)
        end
      end
    end

    local function onError(e)
      return destination:onError(e)
    end

    local function createOnCompleted(i)
      return function()
        completedCount = completedCount + 1

        if completedCount == #sources then
          destination:onCompleted()
        end
      end
    end

    local sink = Observer.create(createOnNext(1), onError, createOnCompleted(1))

    for i = 2, #sources do
      sink:add(sources[i]:subscribe(createOnNext(i), onError, createOnCompleted(i)))
    end

    return sink
  end)
end
