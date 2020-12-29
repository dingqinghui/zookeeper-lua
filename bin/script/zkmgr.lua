
local zookeeper = require("zookeeper")
local coroutines = require("script.coroutines")
local zkclient = require("script.zkclient")




local zkmgr = {
    __clients = {},
    __clientCnt = 0,
    __init = false,
}

local self = zkmgr

local ZKEVENT = {
    CONNECT_EVENT = 1,
    CLOSE_EVENT = 2,
    ASYNRT_EVENT = 3,
    WACHER_EVENT = 4,
}

-- 异步调用类型
local ASYNC_TYPE = {
    ASYNTR_EXIST    = 1,
    ASYNTR_AUTH     = 2,
    ASYNTR_CREATE   = 3,
    ASYNTR_SET      = 4,
    ASYNTR_DELETE   = 5,
    ASYNTR_GETCHILD = 6,
    ASYNTR_GET      = 7,
}

-- 异步调用结果错误码
local ASYNC_ERROR_CODE  = {
    ZKRT_ERROR       = -1 ,
    ZKRT_SUCCESS     = 0 ,
    ZKRT_NONODE      = 1 ,   --节点/父节点不存在
    ZKRT_NODEEXIST   = 2 ,   --节点存在
    ZKRT_AUTHFAIL    = 3 ,   --添加用户失败
}




local function onZkevent2String(zkevent)
    if zkevent == ZKEVENT.CONNECT_EVENT then 
        return "ZKEVENT.CONNECT_EVENT"
    elseif zkevent == ZKEVENT.CLOSE_EVENT then 
        return "ZKEVENT.CLOSE_EVENT"
    elseif zkevent == ZKEVENT.ASYNRT_EVENT then 
        return "ZKEVENT.ASYNRT_EVENT"
    elseif zkevent == ZKEVENT.WACHER_EVENT then 
        return "ZKEVENT.WACHER_EVENT"
    else 
        return "ZKEVENT.UNKNOW"
    end 
end 

local function onAsyncType2String(asyncType)
    if asyncType == ASYNC_TYPE.ASYNTR_EXIST then 
        return "ASYNC_TYPE.ASYNTR_EXIST"
    elseif asyncType == ASYNC_TYPE.ASYNTR_AUTH then 
        return "ASYNC_TYPE.ASYNTR_AUTH"
    elseif asyncType == ASYNC_TYPE.ASYNTR_CREATE then 
        return "ASYNC_TYPE.ASYNTR_CREATE"
    elseif asyncType == ASYNC_TYPE.ASYNTR_SET then 
        return "ASYNC_TYPE.ASYNTR_SET"
    elseif asyncType == ASYNC_TYPE.ASYNTR_DELETE then 
        return "ASYNC_TYPE.ASYNTR_DELETE"
    elseif asyncType == ASYNC_TYPE.ASYNTR_GETCHILD then 
        return "ASYNC_TYPE.ASYNTR_GETCHILD"
    elseif asyncType == ASYNC_TYPE.ASYNTR_GET then 
        return "ASYNC_TYPE.ASYNTR_GET" 
    else 
        return "ASYNC_TYPE.UNKNOW"
    end 
end 

local function  onErrcode2String(errcode)
    if errcode == ASYNC_ERROR_CODE.ZKRT_AUTHFAIL then 
        return "ASYNC_ERROR_CODE.ZKRT_AUTHFAIL"
    elseif errcode == ASYNC_ERROR_CODE.ZKRT_SUCCESS then 
        return "ASYNC_ERROR_CODE.ZKRT_SUCCESS"
    elseif errcode == ASYNC_ERROR_CODE.ZKRT_NONODE then 
        return "ASYNC_ERROR_CODE.ZKRT_NONODE"
    elseif errcode == ASYNC_ERROR_CODE.ZKRT_NODEEXIST then 
        return "ASYNC_ERROR_CODE.ZKRT_NODEEXIST"
    elseif errcode == ASYNC_ERROR_CODE.ZKRT_AUTHFAIL then 
        return "ASYNC_ERROR_CODE.ZKRT_AUTHFAIL"
    else
        return "ASYNC_ERROR_CODE.UNKNOW"
    end 
end 


local zkevent = {}


zkevent[ZKEVENT.CONNECT_EVENT] = function (zkcli,isReconnect)
    local client = zkmgr.getclient(zkcli)
    if not client then 
        return 
    end 
    client:wakeupconnect(isReconnect)
end



zkevent[ZKEVENT.CLOSE_EVENT] = function (zkcli,isExpire)

end

zkevent[ZKEVENT.WACHER_EVENT] = function (zkcli,watcherType,path)
    local client = zkmgr.getclient(zkcli)
    if not client then 
        return 
    end 
    client:wacherHandler(path,watcherType)
end





--[[
    @desc: 
    author:{author}
    time:2020-12-29 17:09:22
    --@zkcli: 
    --@asynType: 异步回调类型
    --@sync: 同步处理还是异步处理
    --@errcode: 回调错误码
    --@...: 
        ASYNTR_AUTH：
        ASYNTR_EXIST：path
        ASYNTR_CREATE：path
        ASYNTR_SET：path
        ASYNTR_DELETE:path
        ASYNTR_GET：path,buffer,bufflen
        ASYNTR_GETCHILD:path,childlist
    @return:
]]
zkevent[ZKEVENT.ASYNRT_EVENT] = function (zkcli,asynType,sync,errcode,...)
    local asyncStr = onAsyncType2String(asynType)
    local errStr = onErrcode2String(errcode)
    print(asyncStr,errStr)
    print("#########",string.format("recv async evnet asynType:%s sync:%d errcode:%s",asyncStr,sync and 1 or 0,errStr),"##########")
        
    local client = zkmgr.getclient(zkcli)
    if not client then 
        return 
    end 
    if sync then 
        local ret = errcode == ASYNC_ERROR_CODE.ZKRT_SUCCESS
        -- 对于创建节点，节点已经存在，也认为创建成功
        if asynType == ASYNC_TYPE.ASYNTR_CREATE then 
            if errcode == ASYNC_ERROR_CODE.ZKRT_NODEEXIST then 
                ret = true
            end 
        end 
        if asynType == ASYNC_TYPE.ASYNTR_DELETE then 
            if errcode == ASYNC_ERROR_CODE.ZKRT_NONODE then 
                ret = true
            end 
        end 

        -- 唤醒协程返回同步结果
        client:asynswakeup(ret,...)
    else
        -- local func = client:pop_async_wacher()
        -- if func then 
        --     func(client,...)
        -- else
        --     print("async not callback")
        -- end 
    end
end


local function zk_callback(zkcli,event,...)
    local eventstr = onZkevent2String(event)
    print("zookeeper event:",eventstr)

    local func = zkevent[event]
    if func then 
        func(zkcli,...)
    else
        print("no find callback event:",eventstr)
    end
end


function zkmgr.init()  
    zookeeper.callback(zk_callback)
end

function zkmgr.newclient(host,timeout,onconnect)
    local client = zkclient.new(host,timeout)

    if not client or client:getid() == nil then
        return 
    end 
    local id = client:getid()
    self.__clients[id] = client
    self.__clientCnt = self.__clientCnt + 1
    coroutines.fork(function() 
        if client:connect(onconnect) then 
            return client
        end 
    end)
    return client
end


function zkmgr.freeclient(client)
    if not client then 
        local id = client:getid()
        self.__clients[id] = nil
        self.__clientCnt = self.__clientCnt - 1
    end 
end

function zkmgr.getclient(id)
    return self.__clients[id]
end


function zkmgr.run()
    if not self.__init then 
        zookeeper.callback(zk_callback)
    end 

    for id,client in pairs(self.__clients) do
        client:run()
    end
end

return zkmgr