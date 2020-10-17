# Contributing

Pull requests are welcome! To learn more about this codebase, code organization, tooling, etc. please read the entire document.

## Organization

Source is contained in the `reactivex` directory.  There is one Lua file for each class or other program unit. All operators are separate modules and they live in the `reactivex/operators` directory.

## Tooling

There are a number of scripts in the `tools` folder that automate certain tasks.  In general, they are run using `lua tools/<file>.lua` from the project root and take no arguments.

### Portable release

Everything in the `reactivex` directory can be combined into a single portable `reactivex.lua` file in the project root. `tools/build.lua` script takes care of that, under the hood it uses [lua-amalg](https://github.com/siffiejoe/lua-amalg) which is also embedded in the `tools/` directory. 

f you want to create a luvit build with some additional metadata embedded, you can run `lua tools/build.lua luvit

When a tag is published in this repository, these steps are done automatically on TravisCI and the resulting portable builds are uploaded as Releases.

> **Note:** This is not a direct file-by-file concatenation, but rather an *amalgamation*. The resulting code will populate the cache of the `require()` by adding entries to `package.preload`. Using `lua-amalg` is required because the old build script could not handle circular dependencies very well as it was just concatenating all files into one big script.

### Documentation
The documentation in `doc/README.md` is automatically generated based on comments in the source. `tools/update_documentation.lua` script performs this generation. Internally it uses `docroc`, which is a library that parses Lua comments and returns them in a table. When its done, `update_documentation.lua` converts the table to markdown and writes it to the `doc` directory.

You should run this script and include an updated `doc/README.md` as part of a pull request.

## Tests

Tests are much welcome when adding new functionality. They protect against regressions when changes are introduced in the future and give us confidence that the code works correctly. 

However, the tests are not a documentation nor example of the usage. They should not be created with such assumption in mind. They can be sometimes used as a reference when trying to understand deeper topics, but to demonstrate the actual usage of the library by end-users, we have `examples/` and the documentation. 

### General rules

- When writing tests, test the **public behavior** of the library, DO NOT check implementation details!

- All tests are running in full isolation so they don't leak - running one test should never influence the outcome of another test, or the coverage report of the class / script which is not being tested by the current test. 

- Always `require()` all the `reactivex.*` classes directly, one by one at the top of the test file. **Don't** use the `init` module, **don't do** `require("reactivex")`. 

- When testing operators, only include the one specific operator you are testing and not any other one
- Not following the last two rules will result in coverage leaks (coverage measurements will be higher than they are in reality).

### Test tooling
We're using [lust](https://github.com/bjornbytes/lust) for testing.

To run the tests, run this command in the root directory of the project:

```
lua tests/runner.lua
```

To run a specific test file, add the test name as a parameter:

```
lua tests/runner.lua average
```


#### Coverage reports

Install  `luacov` and `luacov-reporter-lcov` from luarocks, and also install `lcov` on your system. Then run:

```
lua tests/runner.lua --with-coverage --with-lcov-report
```

This will generate nice HTML reports with `lcov`.<br>

To collect coverage stats only without generating a report, use:

```
lua tests/runner.lua --with-coverage
```

After that you can manually proces the output with other tools. To generate report manually with lcov for example, you can run:

```
luacov -r lcov
genhtml luacov.report.out -o .tmp/coverage
```

### Test helpers

In addition to lust's default operators, there are also a few additional utilities available in the global namespace of each test file:

- **`expect(Observable).to.produce(...)`** -  assert that the Observable produces the specified values, in order.  If you need to assert against multiple values emitted by a single `onNext`, you can pass in a table (i.e. `{{1, 2, 3}, {4, 5, 6}}` to check that an Observable calls `onNext` twice with 3 values each).
- **`expect(Observable).to.produce.error()`** - assert that the Observable produces an error.
- **`expect(Observable).to.produce.nothing()`** - Assert that the observable does not produce anything
- **`local onNext, onError, onCompleted = observableSpy(observable)`** - create three spies for each of the reactive notifications. You can read more about spies on lust's README.
- **`tryCall(fn, errorsAccumulatorTable)`** - safely accumulate errors which will be displayed when done testing
- **`throwErrorsIfAny(errorsAccumulatorTable)`** - throw a batch of accumulated errors (single error with all messages nicely concatenated)
- **`createSingleUseOperator(operatorName, observerFactoryFn)`** -  creates a disposable for testing purposes. After the first time the operator is called, it is automatically disposed. `observerFactoryFn` must return an observer, which will be then used internally to `:lift()` (chain) the observable just like it's done in all operators. Use `createPassThroughObserver()` to simplify the observer creation.
- **`createPassThroughObserver(destination, overrideOnNext, overrideOnError, overrideOnCompleted)`** - creates an observer, which by default acts as a transparent proxy and passes all the notifications from the source observable directly to the `destination` (to the subscriber / observer). You can override the default behavior with `overrideOnNext`, `overrideOnError` and `overrideOnCompleted` parameters by passing custom functions there. They will receive the same parameter as the source observable originally emits.

## Coding style

Aim for a consistent style.

- Wrap lines to 100 characters.
- Indent using two spaces.
- Document functions using `docroc` syntax so documentation gets automatically generated.  In general this means starting your first comment with three dashes, then using `@arg` and `@returns`.
- If you don't want to include some doc comments in the public documentation, use two dashes instead of three,
- Tend to avoid single line `if`s (i.e. `if condition then action end`).
