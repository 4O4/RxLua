local Observable = require("reactivex.observable")
local Observer = require("reactivex.observer")
local Subscription = require("reactivex.subscription")

require('reactivex.operators.pluck')

describe('pluck', function()
  it('returns the original observable if no key is specified', function()
    local observable = Observable.of(7)
    expect(observable:pluck()).to.equal(observable)
  end)

  it('retrieves the specified key from tables produced by the observable', function()
    local t = {
      { color = 'red' },
      { color = 'green' },
      { color = 'blue' }
    }
    expect(Observable.fromTable(t, ipairs):pluck('color')).to.produce('red', 'green', 'blue')
  end)

  it('supports numeric pluckage', function()
    local t = {{1, 2, 3}, {4, 5, 6}, {7, 8, 9}}
    expect(Observable.fromTable(t, ipairs):pluck(2)).to.produce(2, 5, 8)
  end)

  it('supports deep pluckage', function()
    local t = {
      { name = { first = 'Robert', last = 'Paulsen' } },
      { name = { first = 'Bond', last = 'James Bond' } }
    }
    expect(Observable.fromTable(t, ipairs):pluck('name', 'first')).to.produce('Robert', 'Bond')
  end)

  it('errors if a key does not exist', function()
    local california = {}
    expect(Observable.fromTable(california):pluck('water')).to.fail()
  end)

  it('respects metatables', function()
    local t = setmetatable({}, {__index = {a = 'b'}})
    expect(Observable.of(t):pluck('a')).to.produce('b')
  end)
end)
