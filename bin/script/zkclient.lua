

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
    self.__nodeWacherList = {}
    self.__childWacherList = {}
    self.__async_cb_list = {}   -- 异步请求回调函数列表
    self.__ondisconnect = nil
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
function zkclient:connect(onconnect,ondisconnect)
    if not self.__zkcli then 
        return 
    end 

    local ret = zookeeper.connect(self.__zkcli)
    if not ret then 
        return ret
    end
    self.__ondisconnect = ondisconnect
    -- 挂起等待操作完成
    self.__connect_co = coroutines.running()
    local isReconnenct = coroutines.supend(self.__connect_co)
    onconnect(isReconnenct)

    return true
end

function zkclient:disconnect(isexpire)
    if self.__ondisconnect then 
        self.__ondisconnect(isexpire)
    end 
end 

function zkclient:getid()
    return self.__zkcli
end

function zkclient:stop()
    zookeeper.stop(self.__zkcli)
end

function zkclient:run()
    if self.__zkcli then 
        zookeeper.run(self.__zkcli)
    end 
end
---------------------------------------------------------------------------------同步api-----------------------------------------------------------------------------
--[[
    @desc: 
    author:{author}
    time:2020-12-29 17:06:47
    --@path:
	--@value:
	--@valuelen:
	--@istemp:
	--@isseq:
	--@authinfo: 
    @return:true/false
]]
function zkclient:createnode(path,value,valuelen,istemp,isseq,authinfo)
    local  ret = zookeeper.createnode(self.__zkcli,path,value,valuelen,istemp,isseq,authinfo or "",true)
    if not ret then 
        return nil
    end 
    return self:asynsupend()
end
--[[
    @desc: 递归创建 不只接调用C API 了，有bug。利用现在的同步API很容易实现一个递归创建节点。
    author:{author}
    time:2020-12-29 17:07:00
    --@path:
	--@value:
	--@valuelen:
	--@istemp:
	--@isseq: 
    @return:true/false
]]
-- function zkclient:rcreatenode(path,value,valuelen,istemp,isseq,authinfo)
--     local  ret = zookeeper.rcreatenode(self.__zkcli,path,value,valuelen,istemp,isseq,authinfo or "",true)
--     if not ret then 
--         return nil
--     end 
--     return self:asynsupend()
-- end
--[[
    @desc: 
    author:{author}
    time:2020-12-29 17:07:07
    --@path:
	--@value:
	--@valuelen: 
    @return:true/false
]]
function zkclient:setnode(path,value,valuelen)
    local  ret = zookeeper.setnode(self.__zkcli,path,value,valuelen,true)
    if not ret then 
        return nil
    end 
    return self:asynsupend()
end
--[[
    @desc: 
    author:{author}
    time:2020-12-29 17:07:18
    --@path: 
    @return:true/false
]]
function zkclient:delnode(path)
    local  ret = zookeeper.delnode(self.__zkcli,path,true)
    if not ret then 
        return nil
    end 
    return self:asynsupend()
end
--[[
    @desc: 
    author:{author}
    time:2020-12-29 17:07:25
    --@path: 
    @return:child not list ,fail return nil
]]
function zkclient:getchilds(path)
    local  ret = zookeeper.getchilds(self.__zkcli,path,true)
    if not ret then 
        return nil
    end 
    return self:asynsupend()
end
--[[
    @desc: 
    author:{author}
    time:2020-12-29 17:08:16
    --@path: 
    @return:true exist,false nonode
]]
function zkclient:existnode(path)
    local  ret = zookeeper.existnode(self.__zkcli,path,true)
    if not ret then 
        return nil
    end 
    return self:asynsupend()
end
--[[
    @desc: 
    author:{author}
    time:2020-12-29 17:08:36
    --@path: 
    @return:success return buffer,bufflen,fail return nil,0
]]
function zkclient:getnode(path)
    local  ret = zookeeper.getnode(self.__zkcli,path,true)
    if not ret then 
        return nil
    end 
    return self:asynsupend()
end
--[[
    @desc: 
    author:{author}
    time:2020-12-29 17:09:22
    --@authinfo: 
    @return:true/false
]]
function zkclient:addauth(authinfo)
    local  ret = zookeeper.addauth(self.__zkcli,authinfo,true)
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

function zkclient:push_async_wacher(watcher)
    table.insert(self.__async_cb_list,#self.__async_cb_list + 1,watcher) 
end

function zkclient:pop_async_wacher(watcher)
    for k,v in pairs(self.__async_cb_list) do
        print(k,v)
    end
    return table.remove(self.__async_cb_list,1)
end
----------------------------------------------------------------------------同步api----------------------------------------------------------------------------------


function zkclient:agetnode(path,watcher)
    local  ret = zookeeper.getnode(self.__zkcli,path,false)
    if not ret then 
        return false
    end 

    self:push_async_wacher(watcher)
    return true
end

function zkclient:agetchilds(path,watcher)
    local  ret = zookeeper.getchilds(self.__zkcli,path,false)
    if not ret then 
        return false
    end 

    self:push_async_wacher(watcher)
    return true
end

function zkclient:aexistnode(path,watcher)
    local  ret = zookeeper.existnode(self.__zkcli,path,false)
    if not ret then 
        return false
    end 

    self:push_async_wacher(watcher)
    return true
end


function zkclient:subscribeNode(path,func)
    local  ret = zookeeper.nodewatcher(self.__zkcli,path)
    if not ret then 
        print("subscribe node watcher fail. path:",path)
        return ret 
    end 
    self.__nodeWacherList[path] = func
    return true
end
-- 父节点不存在 会在回调中报错
function zkclient:subscribeChildNode(path,func)
    local  ret = zookeeper.childwacher(self.__zkcli,path)
    if not ret then 
        print("subscribe node childwacher fail. path:",path)
        return ret 
    end 
    self.__childWacherList[path] = func
    return true
end



function zkclient:unsubscribe(watchList,path)
    if watchList then 
        watchList[path] = nil
    end
end



function zkclient:getNodeWacherList()
    return self.__nodeWacherList
end

function zkclient:getChildWacherList()
    return self.__childWacherList
end


return zkclient