local Observable = require 'reactivex.observable'
local Observer = require 'reactivex.observer'

--- Returns a new Observable that produces values computed by extracting the given keys from the
-- tables produced by the original.
-- @arg {string...} keys - The key to extract from the table. Multiple keys can be specified to
--                         recursively pluck values from nested tables.
-- @returns {Observable}
function Observable:pluck(key, ...)
  if not key then return self end

  if type(key) ~= 'string' and type(key) ~= 'number' then
    return Observable.throw('pluck key must be a string')
  end

  return self:lift(function (destination)
    local function onNext(t)
      return destination:onNext(t[key])
    end

    local function onError(e)
      return destination:onError(e)
    end

    local function onCompleted()
      return destination:onCompleted()
    end

    return Observer.create(onNext, onError, onCompleted)
  end):pluck(...)
end
