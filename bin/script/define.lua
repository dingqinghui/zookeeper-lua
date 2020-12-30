local __M = {}
__M.WACHER_EVENT_TYPE = {
    EventWacherFail            = -1,
    EventNodeCreated           = 0 ,
    EventNodeDeleted           = 1 ,
    EventNodeDataChanged       = 2 ,
    EventNodeChildrenChanged   = 3 ,
}


__M.ZKEVENT = {
    CONNECT_EVENT = 1,
    CLOSE_EVENT = 2,
    ASYNRT_EVENT = 3,
    WACHER_EVENT = 4,
}

-- 异步调用类型
__M.ASYNC_TYPE = {
    ASYNTR_EXIST    = 1,
    ASYNTR_AUTH     = 2,
    ASYNTR_CREATE   = 3,
    ASYNTR_SET      = 4,
    ASYNTR_DELETE   = 5,
    ASYNTR_GETCHILD = 6,
    ASYNTR_GET      = 7,
}

-- 异步调用结果错误码
__M.ASYNC_ERROR_CODE  = {
    ZKRT_ERROR       = -1 ,
    ZKRT_SUCCESS     = 0 ,
    ZKRT_NONODE      = 1 ,   --节点/父节点不存在
    ZKRT_NODEEXIST   = 2 ,   --节点存在
    ZKRT_AUTHFAIL    = 3 ,   --添加用户失败
}


return  __M