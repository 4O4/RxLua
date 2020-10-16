local Observable = require 'reactivex.observable'
local Subscription = require 'reactivex.subscription'
local util = require 'reactivex.util'
local Observer = require 'reactivex.observer'

--- Returns an Observable that produces values from the original along with the most recently
-- produced value from all other specified Observables. Note that only the first argument from each
-- source Observable is used.
-- @arg {Observable...} sources - The Observables to include the most recent values from.
-- @returns {Observable}
function Observable:with(...)
  local sources = {...}

  return self:lift(function (destination)
    local latest = setmetatable({}, {__len = util.constant(#sources)})

    local function setLatest(i)
      return function(value)
        latest[i] = value
      end
    end

    local function onNext(value)
      return destination:onNext(value, util.unpack(latest))
    end

    local function onError(e)
      return destination:onError(e)
    end

    local function onCompleted()
      return destination:onCompleted()
    end

    local sink = Observer.create(onNext, onError, onCompleted)

    for i = 1, #sources do
      sink:add(sources[i]:subscribe(setLatest(i), util.noop, util.noop))
    end

    return sink
  end)
end
