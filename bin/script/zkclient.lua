

local zookeeper = require("zookeeper")
local coroutines = require("script.coroutines")
local WACHER_EVENT_TYPE = require("script.zkwevent") 


local function onWacherType2String(watcherType)
    if watcherType == WACHER_EVENT_TYPE.EventNodeCreated then 
        return "EventNodeCreated"
    elseif watcherType == WACHER_EVENT_TYPE.EventNodeDeleted then
        return "EventNodeDeleted"
    elseif watcherType == WACHER_EVENT_TYPE.EventNodeDataChanged then
        return "EventNodeDataChanged"
    elseif watcherType == WACHER_EVENT_TYPE.EventNodeChildrenChanged then
        return "EventNodeChildrenChanged"
    else
        return "EventWacherFail"
    end 
end 

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
    self.__wacherlist = {}
    self.__async_cb_list = {}   -- 异步请求回调函数列表
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

    return true
end

function zkclient:getid()
    return self.__zkcli
end

function zkclient:stop()
    zookeeper.stop(self.__zkcli)
end

function zkclient:run()
    if self.__zkcli then 
        print( zookeeper.run(self.__zkcli) )
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



function zkclient:wacherHandler(path,watcherType)
    print("-----------",onWacherType2String(watcherType))
    local func = self:getsubscribewacher(path)
    if not func then 
        return 
    end 

    local callback = function (...)
        print("get wacher node state",...)
        func(watcherType,...)
    end 
    while true do 
        if watcherType == WACHER_EVENT_TYPE.EventNodeCreated or
        watcherType == WACHER_EVENT_TYPE.EventNodeDeleted then
            
            -- 获取最新状态防止 两次注册之间事件丢失
            if not self:aexistnode(path,callback) then  
                break
            end 
            -- 重复注册
            if not self:subscribeNode(path,func)  then 
                break
            end 
            return 
        elseif watcherType == WACHER_EVENT_TYPE.EventNodeDataChanged then
            if not self:agetnode(path,callback) then  
                break
            end 
            if not self:subscribeNode(path,func)  then 
                break
            end 
            return
        elseif watcherType == WACHER_EVENT_TYPE.EventNodeChildrenChanged then
            if not self:agetchilds(path,callback) then  
                break
            end 
            if not self:subscribeChildNode(path,func)  then 
                break
            end 
            return
        else
            break
        end
    end

    self:unsubscribe(path)  -- 取消注册
    func(path,watcherType)  -- 通知逻辑层

end


function zkclient:subscribe(path,func)
    if self.__wacherlist[path] then 
        return 
    end 

    local  ret = zookeeper.nodewatcher(self.__zkcli,path)
    if not ret then 
        print("subscribe node watcher fail. path:",path)
        return ret 
    end 

    ret = zookeeper.childwacher(self.__zkcli,path)
    if not ret then 
        print("subscribe node childwacher fail. path:",path)
        return ret 
    end 

    self.__wacherlist[path] = func
    return true
end


function zkclient:subscribeNode(path,func)
    local  ret = zookeeper.nodewatcher(self.__zkcli,path)
    if not ret then 
        print("subscribe node watcher fail. path:",path)
        return ret 
    end 
    self.__wacherlist[path] = func
    return true
end
-- 父节点不存在 会在回调中报错
function zkclient:subscribeChildNode(path,func)
    local  ret = zookeeper.childwacher(self.__zkcli,path)
    if not ret then 
        print("subscribe node childwacher fail. path:",path)
        return ret 
    end 
    self.__wacherlist[path] = func
    return true
end



function zkclient:unsubscribe(path)
    print("unsubscribe",debug.traceback())
    if self.__wacherlist[path] then 
        self.__wacherlist[path] = nil
    end
end


function zkclient:getsubscribewacher(path)
    return self.__wacherlist[path]
end




return zkclient