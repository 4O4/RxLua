local Observable = require 'reactivex.observable'
local util = require 'reactivex.util'
local Observer = require 'reactivex.observer'

--- Runs a function each time this Observable has activity. Similar to subscribe but does not
-- create a subscription.
-- @arg {function=} onNext - Run when the Observable produces values.
-- @arg {function=} onError - Run when the Observable encounters a problem.
-- @arg {function=} onCompleted - Run when the Observable completes.
-- @returns {Observable}
function Observable:tap(_onNext, _onError, _onCompleted)
  _onNext = _onNext or util.noop
  _onError = _onError or util.noop
  _onCompleted = _onCompleted or util.noop

  return self:lift(function (destination)
    local function onNext(...)
      util.tryWithObserver(destination, function(...)
        _onNext(...)
      end, ...)

      return destination:onNext(...)
    end

    local function onError(message)
      util.tryWithObserver(destination, function()
        _onError(message)
      end)

      return destination:onError(message)
    end

    local function onCompleted()
      util.tryWithObserver(destination, function()
        _onCompleted()
      end)

      return destination:onCompleted()
    end

    return Observer.create(onNext, onError, onCompleted)
  end)
end
