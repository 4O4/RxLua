local util = require 'reactivex.util'

--- @class Subscription
-- @description A handle representing the link between an Observer and an Observable, as well as any
-- work required to clean up after the Observable completes or the Observer unsubscribes.
local Subscription = {}
Subscription.__index = Subscription
Subscription.__tostring = util.constant('Subscription')
Subscription.___isa = { Subscription }

--- Creates a new Subscription.
-- @arg {function=} action - The action to run when the subscription is unsubscribed. It will only
--                           be run once.
-- @returns {Subscription}
function Subscription.create(teardown)
  local self = {
    _unsubscribe = teardown,
    _unsubscribed = false,
    _parentOrParents = nil,
    _subscriptions = nil,
  }

  return setmetatable(self, Subscription)
end

function Subscription:isUnsubscribed()
  return self._unsubscribed
end

--- Unsubscribes the subscription, performing any necessary cleanup work.
function Subscription:unsubscribe()
  if self._unsubscribed then return end

  -- copy some references which will be needed later
  local _parentOrParents = self._parentOrParents
  local _unsubscribe = self._unsubscribe
  local _subscriptions = self._subscriptions

  self._unsubscribed = true
  self._parentOrParents = nil

  -- null out _subscriptions first so any child subscriptions that attempt
  -- to remove themselves from this subscription will gracefully noop
  self._subscriptions = nil

  if util.isa(_parentOrParents, Subscription) then
    _parentOrParents:remove(self)
  elseif _parentOrParents ~= nil then
    for _, parent in ipairs(_parentOrParents) do
      parent:remove(self)
    end
  end

  local errors

  if util.isCallable(_unsubscribe) then
    local success, msg = pcall(_unsubscribe, self)

    if not success then
      errors = { msg }
    end
  end

  if type(_subscriptions) == 'table' then
    local index = 1
    local len = #_subscriptions

    while index <= len do
      local sub = _subscriptions[index]

      if type(sub) == 'table' then
        local success, msg = pcall(function () sub:unsubscribe() end)

        if not success then
          errors = errors or {}
          table.insert(errors, msg)
        end
      end

      index = index + 1
    end
  end

  if errors then
    error(table.concat(errors, '; '))
  end
end

function Subscription:add(teardown)
  if not teardown then
    return Subscription.EMPTY
  end

  local subscription = teardown

  if util.isCallable(teardown)
    and not util.isa(teardown, Subscription)
  then
    subscription = Subscription.create(teardown)
  end

  if type(subscription) == 'table' then
    if subscription == self or subscription._unsubscribed or type(subscription.unsubscribe) ~= 'function' then
      -- This also covers the case where `subscription` is `Subscription.EMPTY`, which is always unsubscribed
      return subscription
    elseif self._unsubscribed then
      subscription:unsubscribe()
      return subscription
    elseif not util.isa(teardown, Subscription) then
      local tmp = subscription
      subscription = Subscription.create()
      subscription._subscriptions = { tmp }
    end
  else
    error('unrecognized teardown ' .. tostring(teardown) .. ' added to Subscription')
  end

  local _parentOrParents = subscription._parentOrParents

  if _parentOrParents == nil then
    subscription._parentOrParents = self
  elseif util.isa(_parentOrParents, Subscription) then
    if _parentOrParents == self then
      return subscription
    end

    subscription._parentOrParents = { _parentOrParents, self }
  else
    local found = false

    for _, existingParent in ipairs(_parentOrParents) do
      if existingParent == self then
        found = true
      end
    end

    if not found then
      table.insert(_parentOrParents, self)
    else
      return subscription
    end
  end

  local subscriptions = self._subscriptions

  if subscriptions == nil then
    self._subscriptions = { subscription }
  else
    table.insert(subscriptions, subscription)
  end

  return subscription
end

function Subscription:remove(subscription)
  local subscriptions = self._subscriptions

  if subscriptions then
    for i, existingSubscription in ipairs(subscriptions) do
      if existingSubscription == subscription then
        table.remove(subscriptions, i)
        return
      end
    end
  end
end

Subscription.EMPTY = (function (sub)
  sub._unsubscribed = true
  return sub
end)(Subscription.create())

return Subscription
