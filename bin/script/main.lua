


local coroutines = require("script.coroutines")
local zkclient = require("script.zkclient")
local zkmgr = require("script.zkmgr")
local zookeeper = require("zookeeper")

local client = nil



--zkmgr.init() 

local function onConnect(isReconnenct)

    print("--------------onConnect--------------------",isReconnenct,coroutines.running())

    print( "createnode",client:createnode("/setnode","ss",3,false,false,"") )
    print( "setnode",client:setnode("/setnode","ss2",4) )
    print( "getnode", client:getnode("/setnode") )
end




client = zkmgr.newclient("192.168.93.130:2183,192.168.93.130:2184,192.168.93.130:2185",5000,onConnect)



while true do
    zkmgr.run()
end




-- local function zk_callback(zkcli,event,...)
--     print("zk_callback",zkcli,event,...)
--     local args = {...}
--     if event == 1 then 
--         -- zookeeper.createnode(zkcli,"/parent1111","1",2,false,false,"")
--         -- zookeeper.rcreatenode(zkcli,"/parent111112/child","1",2,false,false,"")
--         --zookeeper.setnode(zkcli,"/parent111112/child","sa",3)
--         --zookeeper.getnode(zkcli,"/parent111112/child")
--         --zookeeper.getchilds(zkcli,"/parent111112")
--         --zookeeper.existnode(zkcli,"/parent111112")

--         -- zookeeper.nodewatcher(zkcli,"/parent111112")
--         -- zookeeper.childwacher(zkcli,"/parent111112")
--         -- zookeeper.setnode(zkcli,"/parent111112","sa11",3)
--         -- zookeeper.createnode(zkcli,"/parent111112/child1","1",2,false,false,"")

--     elseif event == 3 then
--         if args[1] == 6 then 
--             for i,v in pairs(args[4]) do
--                 print(v)
--             end
--         end 
--     end
-- end

-- zookeeper.callback(zk_callback)


-- local zkcli = zookeeper.new("127.0.0.1:2183,127.0.0.1:2184,127.0.0.1:2185",5000)




-- zookeeper.connect(zkcli)





-- while true do
--     zookeeper.run(zkcli)
-- end

