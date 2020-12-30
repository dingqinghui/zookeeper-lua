


local coroutines = require("script.coroutines")
local zkclient = require("script.zkclient")
local zkmgr = require("script.zkmgr")
local zookeeper = require("zookeeper")
local define = require("script.define") 
local WACHER_EVENT_TYPE = define.WACHER_EVENT_TYPE


local client = nil

function getnodeHandler(...)
    print("getnodeHandler",...)

end 



--zkmgr.init() 
local function test1()
     ---------------------------------------同步API测试-----------------------------------------------
     print( "######## createnode ret:",client:createnode("/setnode","ss",3,false,false,"") )    
     -- ######## createnode ret:        true    /setnode
     print( "######## createnode ret:",client:createnode("/setnode/child","ss11",3,false,false,"") )
     print( "######## setnode ret:",client:setnode("/setnode","ss2",4) )
     --######## setnode ret:   true    /setnode
     local ret,parentpath ,childs = client:getchilds("/setnode")
     for k,child in pairs(childs) do
         local childpath = string.format("%s/%s",parentpath,child)
         print("######## childpath:",childpath)
         print("########",client:getnode(childpath))
     end
     print( "######## getchilds ret:",client:getchilds("/setnode") )
     --######## getchilds ret: true    /setnode        table: 0x1953690
     print( "######## existnode ret:",client:existnode("/setnode") )
     --######## existnode ret: true    /setnode
     print( "######## getnode ret:",client:getnode("/setnode") )
     --######## getnode ret:   true    /setnode        ss2     4
     print( "######## delnode ret:",client:delnode("/setnode/child") )
     --######## delnode ret:   true    /setnode/child
     print( "######## delnode ret:",client:delnode("/setnode") )
     ---------------------------------------同步API测试-----------------------------------------------
end 

 ---------------------------------------监视点测试--------------------------------------------------------
local function test2()

    function wacherHandler(watcherType,path,...)
        print("#############",
            string.format("watcherType:%s path:%s",
                    zkmgr.wacherType2String(watcherType),
                    path
                ),
            ...)
        if watcherType == WACHER_EVENT_TYPE.EventNodeCreated then 
            
        elseif watcherType == WACHER_EVENT_TYPE.EventNodeDeleted then
           
        elseif watcherType == WACHER_EVENT_TYPE.EventNodeDataChanged then
           
        elseif watcherType == WACHER_EVENT_TYPE.EventNodeChildrenChanged then
            
        else
            return "EventWacherFail"
        end 
    end 

    client:subscribeNode("/setnode",wacherHandler)
    client:subscribeChildNode("/setnode",wacherHandler)
end 
 ---------------------------------------监视点测试--------------------------------------------------------

local function onConnect(isReconnenct)
   
   

end




client = zkmgr.newclient("127.0.0.1:2183,127.0.0.1:2184,127.0.0.1:2185",5000,onConnect)



while true do
    zkmgr.run()


end

