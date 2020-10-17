local Observable = require("reactivex.observable")
local Observer = require("reactivex.observer")
local Subscription = require("reactivex.subscription")

require('reactivex.operators.ignoreElements')

describe('ignoreElements', function()
  it('passes through errors from the source', function()
    expect(Observable.throw():ignoreElements()).to.produce.error()
  end)

  it('does not produce any values produced by the source', function()
    expect(Observable.fromRange(3):ignoreElements()).to.produce.nothing()
  end)
end)
