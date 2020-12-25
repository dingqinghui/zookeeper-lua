
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




zkevent[ZKEVENT.ASYNRT_EVENT] = function (zkcli,asynType,...)
    local client = zkmgr.getclient(zkcli)
    if not client then 
        return 
    end 
    if asynType == 7 then 
        
    end
    client:asynswakeup(...)
end


local function zk_callback(zkcli,event,...)
    print(...)
    local func = zkevent[event]
    if func then 
        func(zkcli,...)
    end
end


function zkmgr.init()  
    -- 设定回调 所有zkclient共用
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