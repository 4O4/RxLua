local Observable = require 'reactivex.observable'
local util = require 'reactivex.util'
local Observer = require 'reactivex.observer'

--- Returns a new Observable that produces the values produced by all the specified Observables in
-- the order they are specified.
-- @arg {Observable...} sources - The Observables to concatenate.
-- @returns {Observable}
function Observable:concat(other, ...)
  if not other then return self end

  local others = {...}

  return self:lift(function (destination)
    local function onNext(...)
      return destination:onNext(...)
    end

    local function onError(message)
      return destination:onError(message)
    end

    local function onCompleted()
      return destination:onCompleted()
    end

    local function chain()
      other:concat(util.unpack(others)):subscribe(onNext, onError, onCompleted)
    end

    return Observer.create(onNext, onError, chain)
  end)
end
