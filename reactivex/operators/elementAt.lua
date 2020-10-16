local Observable = require 'reactivex.observable'
local Observer = require 'reactivex.observer'

--- Returns an Observable that produces the nth element produced by the source Observable.
-- @arg {number} index - The index of the item, with an index of 1 representing the first.
-- @returns {Observable}
function Observable:elementAt(index)
  if not index or type(index) ~= 'number' then
    error('Expected a number')
  end

  return self:lift(function (destination)
    local i = 1

    local function onNext(...)
      if i == index then
        destination:onNext(...)
        destination:onCompleted()
      else
        i = i + 1
      end
    end

    local function onError(e)
      return destination:onError(e)
    end

    local function onCompleted()
      return destination:onCompleted()
    end

    return Observer.create(onNext, onError, onCompleted)
  end)
end
