

local coroutine = require("coroutine")
local coroutine_create = coroutine.create
local coroutine_resume = coroutine.resume
local coroutine_running = coroutine.running
local coroutine_yield  = coroutine.yield

local coroutines = {
    __co_set = {}
}

local self = coroutines

function coroutines.fork(func,...)
    local co = coroutine_create(func)
    print("coroutine_resume", coroutine_resume(co,...) )
    return co
end 


function coroutines.running()
    return coroutine_running()
end

function coroutines.supend(co)
    print( "supend isyieldable",coroutine.isyieldable(),co  )
    return  coroutine_yield(co)
end

function coroutines.wakeup(co,...)
    print("coroutine_resume",co)
    return coroutine_resume(co,...)
end




return coroutines