local Observable = require 'reactivex.observable'
local util = require 'reactivex.util'
local Observer = require 'reactivex.observer'

--- Returns an Observable that intercepts any errors from the previous and replace them with values
-- produced by a new Observable.
-- @arg {function|Observable} handler - An Observable or a function that returns an Observable to
--                                      replace the source Observable in the event of an error.
-- @returns {Observable}
function Observable:catch(handler)
  handler = handler and (type(handler) == 'function' and handler or util.constant(handler))

  return self:lift(function (destination)
    local function onNext(...)
      return destination:onNext(...)
    end

    local function onError(e)
      if not handler then
        return destination:onCompleted()
      end

      local success, _continue = pcall(handler, e)

      if success and _continue then
        _continue:subscribe(destination)
      else
        destination:onError(_continue)
      end
    end

    local function onCompleted()
      destination:onCompleted()
    end

    return Observer.create(onNext, onError, onCompleted)
  end)
end
