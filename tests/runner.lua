local lust = require 'tests/lust'
local testHelpers = {}
local runnerUtils = {}

function main()
  runnerUtils.initializeRunner()

  local withCoverage = false
  local withLcovReport = false
  local testVariants = {
    {
      testFile = 'util', 
      luacovConfig = {
        include = { "^.*/reactivex/util$"},
      },
    },
    {
      testFile = 'aliases', 
      luacovConfig = {
        include = { "^.*/reactivex/aliases$"},
      },
    },
    {
      testFile = 'observer', 
      luacovConfig = {
        include = { "^.*/reactivex/observer$" },
      },
    },
    {
      testFile = 'observable', 
      luacovConfig = {
        include = { "^.*/reactivex/observable$"},
      },
    },
    {
      testFile = 'testSubscriptions', 
      luacovConfig = {
        include = { "^.*/reactivex/.*subscription$" },
      },
    },
    {
      testFile = 'subject', 
      luacovConfig = {
        include = { "^.*/reactivex/subjects/subject$"},
      },
    },
    {
      testFile = 'anonymoussubject', 
      luacovConfig = {
        include = { "^.*/reactivex/subjects/anonymoussubject$"},
      },
    },
    {
      testFile = 'asyncsubject', 
      luacovConfig = {
        include = { "^.*/reactivex/subjects/asyncsubject$"},
      },
    },
    {
      testFile = 'behaviorsubject', 
      luacovConfig = {
        include = { "^.*/reactivex/subjects/behaviorsubject$"},
      },
    },
    {
      testFile = 'replaysubject', 
      luacovConfig = {
        include = { "^.*/reactivex/subjects/replaysubject$"},
      },
    },
    {
      testFile = 'testSchedulers', 
      luacovConfig = {
        include = { "^.*/reactivex/schedulers/.*$"},
      },
    },
    {
      testFile = 'testOperators', 
      luacovConfig = {
        include = { "^.*/reactivex/operators/.*$"},
      },
    },
  }
  
  if arg[1] and arg[1] ~= "--with-coverage" then
    if arg[2] then
      os.remove("luacov.report.out")
      os.remove("luacov.stats.out")
      withCoverage = true
    end
  
    runnerUtils.runTestFile(arg[1], arg[2])
  else
  
    if arg[1] == "--with-coverage" then
      os.remove("luacov.report.out")
      os.remove("luacov.stats.out")
      withCoverage = true
    end

    if arg[2] == "--with-lcov-report" then
      withLcovReport = true
    end
  
    for i, variant in ipairs(testVariants) do
      runnerUtils.runTestFile(variant.testFile, withCoverage and variant.luacovConfig or nil)

      if next(testVariants, i) then
        print()
      end
    end
  end
  
  if withLcovReport then
    local luacovRunner = runnerUtils.getLuacovRunner(".luacov")
    luacovRunner.run_report()
    os.execute("mkdir -p .tmp && rm -rf .tmp/coverage && genhtml luacov.report.out -o .tmp/coverage")
  end
  
  local red = string.char(27) .. '[31m'
  local green = string.char(27) .. '[32m'
  local normal = string.char(27) .. '[0m'
  
  if lust.errors > 0 then
    io.write(red .. lust.errors .. normal .. ' failed, ')
  end
  
  print(green .. lust.passes .. normal .. ' passed')
  
  if lust.errors > 0 then os.exit(1) end  
end

function runnerUtils.initializeRunner()
  runnerUtils.installTestHelpersGlobally()
  runnerUtils.extendLustWithReactiveAssertions()
  runnerUtils.installLustGlobally()

  _G.dofile = runnerUtils.dofile
end

function runnerUtils.installLustGlobally()
  for _, fn in pairs({'describe', 'it', 'test', 'expect', 'spy', 'before', 'after'}) do
    _G[fn] = lust[fn]
  end

  _G.lust = lust
end

function runnerUtils.getLuacovRunner(config)
  _G.package.loaded["luacov.runner"] = nil

  local luacovRunner = require("luacov.runner")
  luacovRunner.init(config)

  return luacovRunner
end

function runnerUtils.clean_env(...)
  for k, v in pairs(_G.package.loaded) do
    if k:sub(1, 9) == "reactivex" then
      _G.package.loaded[k] = nil
    end
  end

  _G.Observable = nil
end

local _dofile = dofile
function runnerUtils.dofile(...)
  runnerUtils.clean_env()
  _dofile(...)
end

function runnerUtils.runTestFile(path, luacovConfig)
  if luacovConfig then
    local luacovRunner = runnerUtils.getLuacovRunner(luacovConfig)
    dofile('tests/' .. path .. '.lua')
    luacovRunner.pause()
    luacovRunner.save_stats()
    luacovRunner.initialized = false
  else
    dofile('tests/' .. path .. '.lua')
  end
end

function runnerUtils.installTestHelpersGlobally()
  for k, helper in pairs(testHelpers) do
    _G[k] = helper
  end
end

function runnerUtils.extendLustWithReactiveAssertions()
  lust.paths['produce'] = {
    'nothing',
    'error',
    test = function(observable, ...)
      local args = {...}
      local values
      if type(args[1]) ~= 'table' then
        values = {}
        for i = 1, math.max(#args, 1) do
          table.insert(values, {args[i]})
        end
      else
        values = args[1]
      end
  
      local onNext, onError, onCompleted = observableSpy(observable)
      expect(observable).to.be.an(require("reactivex.observable"))
      expect(onNext).to.equal(values)
      expect(#onError).to.equal(0)
      expect(#onCompleted).to.equal(1)
      return true
    end
  }
  
  lust.paths['nothing'] = {
    test = function(observable)
      local onNext, onError, onCompleted = observableSpy(observable)
      expect(observable).to.be.an(require("reactivex.observable"))
      expect(#onNext).to.equal(0)
      expect(#onError).to.equal(0)
      expect(#onCompleted).to.equal(1)
      return true
    end
  }
  
  lust.paths['error'] = {
    test = function(observable)
      local _, onError = observableSpy(observable)
      expect(observable).to.be.an(require("reactivex.observable"))
      expect(#onError).to.equal(1)
      return true
    end
  }
  
  table.insert(lust.paths['to'], 'produce')
end

-- helper function to safely accumulate errors which will be displayed when done testing
function testHelpers.tryCall(fn, errorsAccumulator)
  local errNum = #errorsAccumulator

  xpcall(fn, function (err)
    table.insert(errorsAccumulator, err)
  end)

  return #errorsAccumulator == errNum
end

function testHelpers.throwErrorsIfAny(errorsAccumulator)
  if #errorsAccumulator > 0 then
    error(table.concat(errorsAccumulator, '\n\t' .. string.rep('\t', lust.level)))
  end
end

function testHelpers.observableSpy(observable)
  local onNextSpy = spy()
  local onErrorSpy = spy()
  local onCompletedSpy = spy()
  local observer = require("reactivex.observer").create(
    function (...) onNextSpy(...) end,
    function (...) onErrorSpy(...) end,
    function () onCompletedSpy() end
  )
  observable:subscribe(observer)
  return onNextSpy, onErrorSpy, onCompletedSpy
end

function testHelpers.createPassThroughObserver(destination, overrideOnNext, overrideOnError, overrideOnCompleted)
  local Observer = require 'reactivex.observer'

  return Observer.create(
    function (...)
      if overrideOnNext then
        return overrideOnNext(...)
      end

      destination:onNext(...)
    end,
    function (...)
      if overrideOnError then
        return overrideOnError(...)
      end

      destination:onError(...)
    end,
    function ()
      if overrideOnCompleted then
        return overrideOnCompleted()
      end

      destination:onCompleted()
    end
  )
end

function testHelpers.createSingleUseOperator(operatorName, observerFactoryFn)
  local Observable = require 'reactivex.observable'

  local fakeOperator = { 
    observerFactoryFn = observerFactoryFn, 
  }

  function fakeOperator.dispose()
    Observable[operatorName] = nil
  end
  
  Observable[operatorName] = function (self)
    local fakeOperator = fakeOperator

    return self:lift(function (destination)
      local observer = fakeOperator.observerFactoryFn(destination) or createPassThroughObserver(destination)

      -- immediately dispose the operator so it can't be used anymore
      fakeOperator.dispose()

      return observer
    end)
  end
  
  return fakeOperator
end

local unpack = table.unpack or unpack
main(unpack(arg))