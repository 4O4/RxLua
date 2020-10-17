package = "ReactiveX"
version = "dev-1"

source = {
  url = "git://github.com/4O4/lua-reactivex.git"
}

description = {
  summary = "Reactive Extensions for Lua (fork of RxLua)",
  detailed = [[
    Lua-ReactiveX gives Lua the power of Observables, which are data structures that represent a stream of values that arrive over time. They're very handy when dealing with events, streams of data, asynchronous requests, and concurrency.

    This is a friendly fork of RxLua. All credits for initial development go to the original author, bjornbytes (https://github.com/bjornbytes/RxLua).

    This fork includes some fixes and features contributed by the community. There are also foundational changes here in order to introduce a proper automatic unsubscription mechanism which was missing and caused unexpected behavior in some cases. These changes are heavily inspired by the RxJS internals, and thus RxJS is considered a reference implementation for all future development of Lua-ReactiveX.
  ]],
  homepage = "https://github.com/4O4/lua-reactivex",
  license = "MIT/X11",
  maintainer = "4O4 <pk.pawelo@gmail.com>"
}

dependencies = { 
  "lua >= 5.1, <= 5.3" 
}

build = {
  type = "builtin",
  modules = { 
    ["reactivex"] = "reactivex/init.lua",

    ["reactivex.aliases"] = "reactivex/aliases.lua",
    ["reactivex.init"] = "reactivex/init.lua",
    ["reactivex.observable"] = "reactivex/observable.lua",
    ["reactivex.observer"] = "reactivex/observer.lua",
    ["reactivex.subscription"] = "reactivex/subscription.lua",
    ["reactivex.subjectsubscription"] = "reactivex/subjectsubscription.lua",
    ["reactivex.util"] = "reactivex/util.lua",

    ["reactivex.operators.init"] = "reactivex/operators/init.lua",
    ["reactivex.operators.partition"] = "reactivex/operators/partition.lua",
    ["reactivex.operators.last"] = "reactivex/operators/last.lua",
    ["reactivex.operators.pack"] = "reactivex/operators/pack.lua",
    ["reactivex.operators.skipUntil"] = "reactivex/operators/skipUntil.lua",
    ["reactivex.operators.find"] = "reactivex/operators/find.lua",
    ["reactivex.operators.takeLast"] = "reactivex/operators/takeLast.lua",
    ["reactivex.operators.compact"] = "reactivex/operators/compact.lua",
    ["reactivex.operators.zip"] = "reactivex/operators/zip.lua",
    ["reactivex.operators.buffer"] = "reactivex/operators/buffer.lua",
    ["reactivex.operators.tap"] = "reactivex/operators/tap.lua",
    ["reactivex.operators.sum"] = "reactivex/operators/sum.lua",
    ["reactivex.operators.with"] = "reactivex/operators/with.lua",
    ["reactivex.operators.delay"] = "reactivex/operators/delay.lua",
    ["reactivex.operators.merge"] = "reactivex/operators/merge.lua",
    ["reactivex.operators.switch"] = "reactivex/operators/switch.lua",
    ["reactivex.operators.take"] = "reactivex/operators/take.lua",
    ["reactivex.operators.scan"] = "reactivex/operators/scan.lua",
    ["reactivex.operators.all"] = "reactivex/operators/all.lua",
    ["reactivex.operators.unwrap"] = "reactivex/operators/unwrap.lua",
    ["reactivex.operators.flatMapLatest"] = "reactivex/operators/flatMapLatest.lua",
    ["reactivex.operators.count"] = "reactivex/operators/count.lua",
    ["reactivex.operators.combineLatest"] = "reactivex/operators/combineLatest.lua",
    ["reactivex.operators.unpack"] = "reactivex/operators/unpack.lua",
    ["reactivex.operators.max"] = "reactivex/operators/max.lua",
    ["reactivex.operators.takeWhile"] = "reactivex/operators/takeWhile.lua",
    ["reactivex.operators.skipWhile"] = "reactivex/operators/skipWhile.lua",
    ["reactivex.operators.flatMap"] = "reactivex/operators/flatMap.lua",
    ["reactivex.operators.min"] = "reactivex/operators/min.lua",
    ["reactivex.operators.startWith"] = "reactivex/operators/startWith.lua",
    ["reactivex.operators.flatten"] = "reactivex/operators/flatten.lua",
    ["reactivex.operators.skipLast"] = "reactivex/operators/skipLast.lua",
    ["reactivex.operators.skip"] = "reactivex/operators/skip.lua",
    ["reactivex.operators.ignoreElements"] = "reactivex/operators/ignoreElements.lua",
    ["reactivex.operators.contains"] = "reactivex/operators/contains.lua",
    ["reactivex.operators.takeUntil"] = "reactivex/operators/takeUntil.lua",
    ["reactivex.operators.concat"] = "reactivex/operators/concat.lua",
    ["reactivex.operators.window"] = "reactivex/operators/window.lua",
    ["reactivex.operators.first"] = "reactivex/operators/first.lua",
    ["reactivex.operators.reject"] = "reactivex/operators/reject.lua",
    ["reactivex.operators.amb"] = "reactivex/operators/amb.lua",
    ["reactivex.operators.reduce"] = "reactivex/operators/reduce.lua",
    ["reactivex.operators.debounce"] = "reactivex/operators/debounce.lua",
    ["reactivex.operators.elementAt"] = "reactivex/operators/elementAt.lua",
    ["reactivex.operators.catch"] = "reactivex/operators/catch.lua",
    ["reactivex.operators.defaultIfEmpty"] = "reactivex/operators/defaultIfEmpty.lua",
    ["reactivex.operators.pluck"] = "reactivex/operators/pluck.lua",
    ["reactivex.operators.retry"] = "reactivex/operators/retry.lua",
    ["reactivex.operators.filter"] = "reactivex/operators/filter.lua",
    ["reactivex.operators.map"] = "reactivex/operators/map.lua",
    ["reactivex.operators.distinctUntilChanged"] = "reactivex/operators/distinctUntilChanged.lua",
    ["reactivex.operators.sample"] = "reactivex/operators/sample.lua",
    ["reactivex.operators.average"] = "reactivex/operators/average.lua",
    ["reactivex.operators.distinct"] = "reactivex/operators/distinct.lua",

    ["reactivex.schedulers.timeoutscheduler"] = "reactivex/schedulers/timeoutscheduler.lua",
    ["reactivex.schedulers.cooperativescheduler"] = "reactivex/schedulers/cooperativescheduler.lua",
    ["reactivex.schedulers.immediatescheduler"] = "reactivex/schedulers/immediatescheduler.lua",

    ["reactivex.subjects.anonymoussubject"] = "reactivex/subjects/anonymoussubject.lua",
    ["reactivex.subjects.asyncsubject"] = "reactivex/subjects/asyncsubject.lua",
    ["reactivex.subjects.behaviorsubject"] = "reactivex/subjects/behaviorsubject.lua",
    ["reactivex.subjects.replaysubject"] = "reactivex/subjects/replaysubject.lua",
    ["reactivex.subjects.subject"] = "reactivex/subjects/subject.lua",
  },
  copy_directories = { "doc", "tests" }
}
