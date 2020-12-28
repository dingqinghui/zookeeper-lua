

local zookeeper = require("zookeeper")
local coroutines = require("script.coroutines")


local zkclient = {}

function zkclient.new(...)
    local obj = setmetatable({}, {
        __index = zkclient,
        __gc = function (self)
            if self.dctor then 
                self:dctor()
            end 
        end
    })
    obj:actor(...)
    return obj
end

function zkclient:actor(host,timeout)
    self.__connect_co = nil
    self.__sync_co_set = {}
    self.__zkcli = nil

    self:load(host,timeout)
end

function zkclient:dctor()
    if self.__zkcli  then 
        zookeeper.destory(self.__zkcli)
    end 
end 

function zkclient:load(host,timeout)
    if not self.__zkcli then 
        self.__zkcli = zookeeper.new(host,timeout) -- 
    end
end 

-- 阻塞连接
function zkclient:connect(onconnect)
    coroutines.fork(function() 
        if not self.__zkcli then 
            return 
        end 

        local ret = zookeeper.connect(self.__zkcli)
        if not ret then 
            return ret
        end

        -- 挂起等待操作完成
        self.__connect_co = coroutines.running()
        local isReconnenct = coroutines.supend(self.__connect_co)
        onconnect(isReconnenct)
    end)
    return true
end

function zkclient:getid()
    return self.__zkcli
end

function zkclient:stop()
    zookeeper.stop(self.__zkcli)
end

function zkclient:run()
    zookeeper.run(self.__zkcli)
end


function zkclient:createnode(path,value,valuelen,istemp,isseq,authinfo)
    local  ret = zookeeper.createnode(self.__zkcli,path,value,valuelen,istemp,isseq,authinfo or "")
    if not ret then 
        return nil
    end 
    return self:asynsupend()
end

function zkclient:rcreatenode(path,value,valuelen,istemp,isseq)
    local  ret = zookeeper.rcreatenode(self.__zkcli,path,value,valuelen,istemp,isseq)
    if not ret then 
        return nil
    end 
    return self:asynsupend()
end

function zkclient:setnode(path,value,valuelen)
    local  ret = zookeeper.setnode(self.__zkcli,path,value,valuelen)
    if not ret then 
        return nil
    end 
    return self:asynsupend()
end

function zkclient:delnode(path)
    local  ret = zookeeper.delnode(self.__zkcli,path)
    if not ret then 
        return nil
    end 
    return self:asynsupend()
end

function zkclient:getchilds(path)
    local  ret = zookeeper.getchilds(self.zkcli,path)
    if not ret then 
        return nil
    end 
    return self:asynsupend()
end

function zkclient:existnode(path)
    local  ret = zookeeper.existnode(self.__zkcli,path)
    if not ret then 
        return nil
    end 
    return self:asynsupend()
end

function zkclient:getnode(path)
    local  ret = zookeeper.getnode(self.__zkcli,path)
    if not ret then 
        return nil
    end 
    return self:asynsupend()
end

function zkclient:addauth(authinfo)
    local  ret = zookeeper.getnode(self.__zkcli,authinfo)
    if not ret then 
        return nil
    end 
    return self:asynsupend()
end



-- 挂起协程等待结果
function zkclient:asynsupend()
    local co = coroutines.running()
    table.insert( self.__sync_co_set, 1 ,co )
    return coroutines.supend(co)
end

function zkclient:asynswakeup(...)
    local co = table.remove(self.__sync_co_set,#self.__sync_co_set)
    if not co then 
        return 
    end 
    coroutines.wakeup(co,...)
end

function zkclient:wakeupconnect(isReconnect)
    if not self.__connect_co then 
        return 
    end 
    coroutines.wakeup(self.__connect_co,isReconnect)
    self.__connect_co = nil
end

return zkclient