local Observable = require 'reactivex.observable'
local Subscription = require 'reactivex.subscription'
local util = require 'reactivex.util'
local Observer = require 'reactivex.observer'

--- Returns a new Observable that produces its most recent value every time the specified observable
-- produces a value.
-- @arg {Observable} sampler - The Observable that is used to sample values from this Observable.
-- @returns {Observable}
function Observable:sample(sampler)
  if not sampler then error('Expected an Observable') end

  return self:lift(function (destination)
    local latest = {}

    local function setLatest(...)
      latest = util.pack(...)
    end

    local function onNext()
      if #latest > 0 then
        return destination:onNext(util.unpack(latest))
      end
    end

    local function onError(message)
      return destination:onError(message)
    end

    local function onCompleted()
      return destination:onCompleted()
    end

    local sink = Observer.create(setLatest, onError)
    sink:add(sampler:subscribe(onNext, onError, onCompleted))

    return sink
  end)
end
