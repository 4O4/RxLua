local Subscription = require("reactivex.subscription")
local util = require("reactivex.util")

-- @class SubjectSubscription
-- @description A specialized Subscription for Subjects. **This is NOT a public class, 
-- it is intended for internal use only!**<br>
-- A handle representing the link between an Observer and a Subject, as well as any
-- work required to clean up after the Subject completes or the Observer unsubscribes.
local SubjectSubscription = setmetatable({}, Subscription)
SubjectSubscription.__index = SubjectSubscription
SubjectSubscription.__tostring = util.constant('SubjectSubscription')

--- Creates a new SubjectSubscription.
-- @arg {Subject} subject - The action to run when the subscription is unsubscribed. It will only
--                           be run once.
-- @returns {Subscription}
function SubjectSubscription.create(subject, observer)
  local self = setmetatable(Subscription.create(), SubjectSubscription)
  self._subject = subject
  self._observer = observer

  return self
end

function SubjectSubscription:unsubscribe()
  if self._unsubscribed then
    return
  end

  self._unsubscribed = true

  local subject = self._subject
  local observers = subject.observers

  self._subject = nil

  if not observers
    or #observers == 0
    or subject.stopped
    or subject._unsubscribed
  then
    return
  end

  for i = 1, #observers do
    if observers[i] == self._observer then
      table.remove(subject.observers, i)
      return
    end
  end
end

return SubjectSubscription
