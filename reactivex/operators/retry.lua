local Observable = require 'reactivex.observable'
local Observer = require 'reactivex.observer'

--- Returns an Observable that restarts in the event of an error.
-- @arg {number=} count - The maximum number of times to retry.  If left unspecified, an infinite
--                        number of retries will be attempted.
-- @returns {Observable}
function Observable:retry(count)
  return self:lift(function (destination)
    local subscription
    local sink
    local retries = 0

    local function onNext(...)
      return destination:onNext(...)
    end

    local function onCompleted()
      return destination:onCompleted()
    end

    local function onError(message)
      if subscription then
        subscription:unsubscribe()
        sink:remove(subscription)
      end

      retries = retries + 1
      if count and retries > count then
        return destination:onError(message)
      end

      subscription = self:subscribe(onNext, onError, onCompleted)
      sink:add(subscription)
    end

    sink = Observer.create(onNext, onError, onCompleted)

    return sink
  end)
end
