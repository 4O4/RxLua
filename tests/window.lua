local Observable = require("reactivex.observable")
local Observer = require("reactivex.observer")
local Subscription = require("reactivex.subscription")
local Subject = require("reactivex.subjects.subject")

require('reactivex.operators.window')

describe('window', function()
  it('produces an error if its parent errors', function()
    createSingleUseOperator(
      "simulateError", 
      function (destination)
        destination:onError()
      end
    )
    local observable = Observable.of(''):simulateError()
    expect(observable).to.produce.error()
    expect(observable:window(2)).to.produce.error()
  end)

  it('fails if size is not specified', function()
    expect(function () Observable.fromRange(5):window() end).to.fail()
  end)

  it('produces a specified number of the most recent values', function()
    expect(Observable.fromRange(3):window(2)).to.produce({{1, 2}, {2, 3}})
    expect(Observable.fromRange(3):window(3)).to.produce({{1, 2, 3}})
  end)
end)
