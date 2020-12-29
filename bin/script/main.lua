


local coroutines = require("script.coroutines")
local zkclient = require("script.zkclient")
local zkmgr = require("script.zkmgr")
local zookeeper = require("zookeeper")




local client = nil

function getnodeHandler(...)
    print("getnodeHandler",...)

end 

function wacherHandler(...)
    print("wacherHandler",...)
end 

--zkmgr.init() 

local function onConnect(isReconnenct)
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


    --print( "createnode", )
    --print( "setnode",client:setnode("/setnode","ss2",4) )
    
    --print( "getnode", client:agetnode("/setnode",getnodeHandler) )

    -- client:subscribeNode("/setnode",wacherHandler)

    -- client:setnode("/setnode","ss9",4)

    -- client:setnode("/setnode","ss1",4)


end




client = zkmgr.newclient("127.0.0.1:2183,127.0.0.1:2184,127.0.0.1:2185",5000,onConnect)



while true do
    zkmgr.run()


end

