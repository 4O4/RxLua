local Observable = require 'reactivex.observable'
local Subscription = require 'reactivex.subscription'
local Observer = require 'reactivex.observer'

--- Returns a new Observable that produces the values produced by all the specified Observables in
-- the order they are produced.
-- @arg {Observable...} sources - One or more Observables to merge.
-- @returns {Observable}
function Observable:merge(...)
  local sources = {...}
  table.insert(sources, 1, self)

  return self:lift(function (destination)
    local completedCount = 0
    local subscriptions = {}

    local function onNext(...)
      return destination:onNext(...)
    end

    local function onError(message)
      return destination:onError(message)
    end

    local function onCompleted(i)
      return function()
        completedCount = completedCount + 1

        if completedCount == #sources then
          destination:onCompleted()
        end
      end
    end

    local sink = Observer.create(onNext, onError, onCompleted(1))

    for i = 2, #sources do
      sink:add(sources[i]:subscribe(onNext, onError, onCompleted(i)))
    end

    return sink
  end)
end
