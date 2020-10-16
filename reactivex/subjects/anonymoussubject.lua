local util = require("reactivex.util")
local Subject -- lazy loaded to avoid loop
local Subscription = require("reactivex.subscription")
local _initialized = false

-- @class AnonymousSubject
-- @description A specialized Subject which acts as a proxy when lifting a Subject. 
-- **This is NOT a public class, it is intended for internal use only!**<br>
-- Its role is crucial to create a proper chain of operators / observables and to make
-- automatic unsubscription work correctly.
local AnonymousSubject = {}
AnonymousSubject.__index = AnonymousSubject
AnonymousSubject.__tostring = util.constant('AnonymousSubject')

local function lazyInitClass()
  if _initialized then return end
  Subject = require("reactivex.subjects.subject")
  setmetatable(AnonymousSubject, Subject)
  _initialized = true
end

function AnonymousSubject.create(sourceSubject, createObserver)
  lazyInitClass()

  local self = setmetatable(Subject.create(), AnonymousSubject)
  self._sourceSubject = sourceSubject
  self._createObserver = createObserver

  return self
end

function AnonymousSubject:onNext(...)
  if self._sourceSubject and self._sourceSubject.onNext then
    self._sourceSubject:onNext(...)
  end
end

function AnonymousSubject:onError(msg)
  if self._sourceSubject and self._sourceSubject.onError then
    self._sourceSubject:onError(msg)
  end
end

function AnonymousSubject:onCompleted()
  if self._sourceSubject and self._sourceSubject.onCompleted then
    self._sourceSubject:onCompleted()
  end
end

function AnonymousSubject:_subscribe(destination)
  if self._sourceSubject then
    return self._sourceSubject:_subscribe(self._createObserver(destination))
  else
    return Subscription.EMPTY
  end
end

return AnonymousSubject
