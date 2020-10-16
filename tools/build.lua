--- Less horrible :) script to bundle all sources into a single portable Lua file.
-- It uses lua-amalg to scan for `require()`d modules and prepare intermediate output
-- file which is then used to produce final files with metadata in two variants:
-- one for more-or-less standard Lua envs (like LOVE 2D) and separate one for Luvit.
--
-- @usage lua tools/build.lua

local VERSION = os.getenv("LUA_REACTIVEX_VERSION") or "0.1.0"

local MAIN = [[
return require('reactivex.init')
]]

local HEADER = [[
-- Lua-ReactiveX v]] .. VERSION .. [[ (Portable single-file build)
-- https://github.com/4O4/lua-reactivex
-- MIT License

]]

local LUVIT_METADATA = [[
exports.name = '4O4/lua-reactivex'
exports.version = ']] .. VERSION .. [['
exports.description = 'Reactive Extensions for Lua (fork of RxLua)'
exports.license = 'MIT'
exports.author = { url = 'https://github.com/4O4' }
exports.homepage = 'https://github.com/4O4/lua-reactivex'

]]

function withFile(path, opts, doCallback)
  local file = io.open(path, opts)
  assert(file)
  doCallback(file)
  file:close()
end

for _, path in ipairs({
  ".tmp/lua-reactivex-portable-luvit/reactivex.lua",
  ".tmp/lua-reactivex-portable/reactivex.lua",
  ".tmp/lua-reactivex-portable-luvit",
  ".tmp/lua-reactivex-portable",
  ".tmp/main.lua",
  ".tmp/out.lua",
  ".tmp",
})
do
  os.remove(path)
end

os.execute("mkdir -p .tmp/lua-reactivex-portable-luvit .tmp/lua-reactivex-portable")

withFile(".tmp/main.lua", "w", function (file)
  file:write(MAIN)
end)

assert(os.execute("lua -ltools/amalg .tmp/main.lua") == 0)
assert(os.execute("lua tools/amalg.lua -o .tmp/out.lua -c -s .tmp/main.lua") == 0)

local amalgOut

withFile(".tmp/out.lua", "r", function (file)
  amalgOut = file:read("*a")
end)

withFile(".tmp/lua-reactivex-portable/reactivex.lua", "w", function (file)
  file:write(table.concat({HEADER, amalgOut}, ""))
end)

withFile(".tmp/lua-reactivex-portable-luvit/reactivex.lua", "w", function (file)
  file:write(table.concat({HEADER, LUVIT_METADATA, amalgOut}, ""))
end)

-- os.exit(0)