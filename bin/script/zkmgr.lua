
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


local on_async_retdeal = {
    [ASYNC_TYPE.ASYNTR_GET] = function (path,errcode,buff,bufflen)
        if errcode == ASYNC_ERROR_CODE.ZKRT_SUCCESS then 
            return buff,bufflen
        else
            return nil,0
        end
    end,
    [ASYNC_TYPE.ASYNTR_GETCHILD] = function (path,errcode,childlist)
        if errcode == ASYNC_ERROR_CODE.ZKRT_SUCCESS then 
            return childlist
        end
        return nil
    end
}


zkevent[ZKEVENT.ASYNRT_EVENT] = function (zkcli,asynType,...)
    
    local client = zkmgr.getclient(zkcli)
    if not client then 
        return 
    end 
    local retList = {... }

    local path = retList[1]
    local errcode = retList[2]

    local ret = nil
    if on_async_retdeal[asynType]  then
        ret = on_async_retdeal[asynType](...)
    else
        ret = errcode == ASYNC_ERROR_CODE.ZKRT_SUCCESS
    end

    client:asynswakeup(ret)
end


local function zk_callback(zkcli,event,...)
    local func = zkevent[event]
    if func then 
        func(zkcli,...)
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

    if client:connect(onconnect) then 
        return client
    end 
    return 
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