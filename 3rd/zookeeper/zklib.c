
#include "../lua/src/lua.h"
#include "../lua/src/lauxlib.h"
#include "../lua/src/lualib.h"
  
#include <assert.h>
#include <stdio.h>
#include "zkcli.h"
  

#define ZK_EVENT_CONNECT 1      
#define ZK_EVENT_CLOSE   2
#define ZK_EVENT_ASYNRT  3    //异步回调事件
#define ZK_EVENT_WATCHER 4    //监视点触发事件


#define ASYNTR_EXIST    1
#define ASYNTR_AUTH     2
#define ASYNTR_CREATE   3
#define ASYNTR_SET      4
#define ASYNTR_DELETE   5
#define ASYNTR_GETCHILD   6
#define ASYNTR_GET 7


#define TO_ZKCLIENT(L,index) \
assert( lua_islightuserdata(L,index) ); \
zkclient* zkcli = lua_touserdata(L,index++);\
assert(zkcli);

#define ZKCLIENT zkcli


#define STACK_TOP_INDEX -1
#define STACK_BOTTOM_INDEX 1


static int CALLBACK_INDEX = LUA_NOREF ;
static int err_fun_stack_index = 0;

int pcall_callback_err_fun(lua_State* L)
{
    lua_Debug debug= {};
    int ret = lua_getstack(L, 2, &debug); // 0是pcall_callback_err_fun自己, 1是error函数, 2是真正出错的函数
    lua_getinfo(L, "Sln", &debug);


    const char* err = lua_tostring(L, -1);
    lua_pop(L, 1);

    printf("%s:line %d err:%s\n",debug.short_src,debug.currentline,err);

    //if (debug.name != 0) {
    //msg << "(" << debug.namewhat << " " << debug.name << ")";
    return 0;
}




static int __pushcb(lua_State *L){
    if(CALLBACK_INDEX == LUA_NOREF){
         return -1;
    }
    lua_pushcfunction(L, pcall_callback_err_fun);
    err_fun_stack_index = lua_gettop(L);
    lua_rawgeti(L, LUA_REGISTRYINDEX, CALLBACK_INDEX);
    assert( lua_isfunction(L,STACK_TOP_INDEX) );
    return 0;
}


static int __callcb(lua_State *L,int args){
    int code = lua_pcall(L,args,0,err_fun_stack_index);
    if ( code != LUA_OK){
       
    }
}

static void __connectedHandler(zkclient* cli,int isReconnect){
    lua_State *L = zkclientGetUserData(cli);

     __pushcb(L) ;

    lua_pushlightuserdata(L,cli);
    lua_pushinteger(L,ZK_EVENT_CONNECT);


    lua_pushboolean(L,isReconnect);
    __callcb(L,3);
}

static void __closeHandler(zkclient* cli,int isExpire){
    lua_State *L = zkclientGetUserData(cli);
    __pushcb(L) ;

    lua_pushlightuserdata(L,cli);
    lua_pushinteger(L,ZK_EVENT_CLOSE);

    lua_pushboolean(L,isExpire);


    __callcb(L,3);
}



static void __existRTHandler(zkclient* cli, int errCode,const char* path,const struct Stat *stat,const void *data){
    int sync = data;
    lua_State *L = zkclientGetUserData(cli);
    __pushcb(L);

    lua_pushlightuserdata(L,cli);
    lua_pushinteger(L,ZK_EVENT_ASYNRT);
    lua_pushinteger(L,ASYNTR_EXIST);
    
    lua_pushboolean(L,sync);
    lua_pushstring(L,path);
    lua_pushinteger(L,errCode);

    __callcb(L,6);
}


static  void __addAuthRTHandler(zkclient* cli,int errCode,void* context){
    lua_State *L = zkclientGetUserData(cli);
    __pushcb(L);

    lua_pushlightuserdata(L,cli);
    lua_pushinteger(L,ZK_EVENT_ASYNRT);
    lua_pushinteger(L,ASYNTR_AUTH);

    lua_pushinteger(L,errCode);
    __callcb(L,4);
}


static  void __createNodeRTHandler(zkclient* cli,int errCode,const char* path,const char* value,void* context){
    lua_State *L = zkclientGetUserData(cli);
    __pushcb(L);

    lua_pushlightuserdata(L,cli);
    lua_pushinteger(L,ZK_EVENT_ASYNRT);
    lua_pushinteger(L,ASYNTR_CREATE);

    lua_pushstring(L,path);
    lua_pushinteger(L,errCode);

    //lua_pushstring(L,value);
 
    __callcb(L,5);
}
static  void __setNodeRTHandler(zkclient* cli,int errCode,const char* path,const struct Stat *stat,void* context){
     lua_State *L = zkclientGetUserData(cli);
    __pushcb(L);

    lua_pushlightuserdata(L,cli);
    lua_pushinteger(L,ZK_EVENT_ASYNRT);
    lua_pushinteger(L,ASYNTR_SET);

    lua_pushstring(L,path);
    lua_pushinteger(L,errCode);

    __callcb(L,5);

}
static  void __getNodeRTHandler(zkclient *cli, int errCode, const char *path, const char *buff, int bufflen, const struct Stat *stat, void *context){
    int sync = context;

    lua_State *L = zkclientGetUserData(cli);
    __pushcb(L);

    lua_pushlightuserdata(L,cli);
    lua_pushinteger(L,ZK_EVENT_ASYNRT);
    lua_pushinteger(L,ASYNTR_GET);

    lua_pushboolean(L,sync);
    lua_pushstring(L,path);
    lua_pushinteger(L,errCode);
    
    lua_pushstring(L,buff);
    lua_pushinteger(L,bufflen);

    __callcb(L,8);
}
static  void __deleteNodeRTHandler(zkclient *cli, int errCode, const char *path, void *context){
     lua_State *L = zkclientGetUserData(cli);
    __pushcb(L);

    lua_pushlightuserdata(L,cli);
    lua_pushinteger(L,ZK_EVENT_ASYNRT);
    lua_pushinteger(L,ASYNTR_DELETE);

    lua_pushstring(L,path);
    lua_pushinteger(L,errCode);

     __callcb(L,5);
}

static  void __getChildrenNodeRTHandler(zkclient *cli, int errCode, const char *path, const struct String_vector *strings, void *context){
    int sync = context;
    lua_State *L = zkclientGetUserData(cli);
    __pushcb(L);

    lua_pushlightuserdata(L,cli);
    lua_pushinteger(L,ZK_EVENT_ASYNRT);
    lua_pushinteger(L,ASYNTR_GETCHILD);

    lua_pushboolean(L,sync);
    lua_pushstring(L,path);
    lua_pushinteger(L,errCode);


    if(strings && strings->count > 0){
        lua_newtable(L);
        for(int i = 0;i < strings->count;++i){
            lua_pushnumber(L, i+1); //将key先压入栈
            lua_pushstring(L,  strings->data[i]); //再将value压入栈
            lua_settable(L, -3);//settable将操作-2，-1编号的键值对，设置到table中，并把key-value从栈中移除
        }
         __callcb(L,7);
    }
    else{
         __callcb(L,6);
    }
}

static  void __nodeEventHandler(zkclient *cli, int eventType, const char *path, void *context){
    lua_State *L = zkclientGetUserData(cli);
    __pushcb(L);

    lua_pushlightuserdata(L,cli);
    lua_pushinteger(L,ZK_EVENT_WATCHER);
    lua_pushinteger(L,eventType);

    lua_pushstring(L,path);
    
    __callcb(L,4);
}


static int zookeeper_callback (lua_State *L) {
    
    CALLBACK_INDEX = luaL_ref(L,LUA_REGISTRYINDEX);
   
    return 0;
}

//Lua主线程调用
static int zookeeper_create (lua_State *L) {
    assert( lua_isstring(L,1) );
    const char* host = lua_tostring(L,1);

    int timeout = lua_tointeger(L,2);

    zkclient* zkcli  = zkclientCreate(host,__connectedHandler,__closeHandler,timeout);
    assert(zkcli);

    //保存一份lua_State
    zkclientSetUserData(zkcli,L);

    lua_pushlightuserdata(L,zkcli);
    return 1;
}
  



static int zookeeper_connect (lua_State *L) {
    int index = 1;
    TO_ZKCLIENT(L,index) 
    int ret = zkclientConnect(ZKCLIENT);
    lua_pushboolean(L,ret == ZK_OK);
    return 1;
}

static int zookeeper_run (lua_State *L) {
    int index = 1;
    TO_ZKCLIENT(L,index) 
    int ret = zkclientRun(ZKCLIENT);
    lua_pushboolean(L,ret == ZK_OK);
    return 1;
}


static int zookeeper_destory (lua_State *L) {
    int index = 1;
    TO_ZKCLIENT(L,index) 
    zkclientFree(ZKCLIENT);
    luaL_unref(L,LUA_REGISTRYINDEX,CALLBACK_INDEX);
    return 0;
}





static int zookeeper_disconnect (lua_State *L) {
     int index = 1;
    TO_ZKCLIENT(L,index) 
    int ret = zkclientDisconnect(ZKCLIENT);
    lua_pushboolean(L,ret == ZK_OK);
    return 1;
}

static int zookeeper_isconnect (lua_State *L) {
    int index = 1;
    TO_ZKCLIENT(L,index) 
    lua_pushboolean(L,zkclientIsConnected(ZKCLIENT));
    return 1;
}

static int zookeeper_stop (lua_State *L) {
     int index = 1;
    TO_ZKCLIENT(L,index) 
    zkclientStop(ZKCLIENT);
    return 0;
}



static int zookeeper_addauth (lua_State *L) {
    int index = 1;
    TO_ZKCLIENT(L,index) 

    assert( lua_isstring(L,index) );
    const char* authinfo = lua_tostring(L,index++); 

    int ret = zkclientAddAuth(ZKCLIENT, authinfo,__addAuthRTHandler,0);
    lua_pushboolean(L,ret == ZK_OK);
    return 1;
}



static int  onCreateNode(lua_State *L,int isRecursive){
    int index = 1;
    TO_ZKCLIENT(L,index); 


    assert( lua_isstring(L,index) );
    const char* path = lua_tostring(L,index++); 

    assert( lua_isstring(L,index) );
    const char* data = lua_tostring(L,index++); 

    assert( lua_isinteger(L,index) );
    int len = lua_tointeger(L,index++); 

    assert( lua_isboolean(L,index) );
    int isTmp = lua_toboolean(L,index++); 

    assert( lua_isboolean(L,index) );
    int isSeq = lua_toboolean(L,index++); 

    assert( lua_isstring(L,index) );
    char* authinfo = lua_tostring(L,index++); 

    if(authinfo[0] == '\0'){
        authinfo = 0;
    }

    if(isRecursive){
        return zkclientRecursiveCreateNode(ZKCLIENT,path,data,len,isTmp,isSeq,__createNodeRTHandler,0, authinfo);
    }
    else{
        return zkclientCreateNode(ZKCLIENT,path,data,len,isTmp,isSeq,__createNodeRTHandler,0, authinfo);
    }
}

static int zookeeper_createnode (lua_State *L) {
    lua_pushboolean(L,onCreateNode(L,0) == ZK_OK);
    return 1;
}
static int zookeeper_recursive_createnode (lua_State *L) {
    lua_pushboolean(L,onCreateNode(L,1) == ZK_OK);
    return 1;
}


static int zookeeper_setnode (lua_State *L) {
    int index = 1;
    TO_ZKCLIENT(L,index); 

    assert( lua_isstring(L,index) );
    const char* path = lua_tostring(L,index++); 

    assert( lua_isstring(L,index) );
    const char* data = lua_tostring(L,index++); 

    assert( lua_isinteger(L,index) );
    int len = lua_tointeger(L,index++); 

    int ret = zkclientSetNode(ZKCLIENT,path,data,len,__setNodeRTHandler,0);
    lua_pushboolean(L,ret == ZK_OK);
    return 1;
}



static int zookeeper_getnode (lua_State *L) {
    int index = 1;
    TO_ZKCLIENT(L,index); 

    assert( lua_isstring(L,index) );
    const char* path = lua_tostring(L,index++); 

    int sync = lua_toboolean(L,index++);

    int ret = zkclientGetNode(ZKCLIENT,path,__getNodeRTHandler,sync);
    printf("zookeeper_getnode ret:%d\n",ret);
    lua_pushboolean(L,ret == ZK_OK);
    return 1;
}





static int zookeeper_delnode (lua_State *L) {
    int index = 1;
    TO_ZKCLIENT(L,index); 

    assert( lua_isstring(L,index) );
    const char* path = lua_tostring(L,index++); 

    int ret = zkclientDelNode(ZKCLIENT,path,__deleteNodeRTHandler,0);
    lua_pushboolean(L,ret == ZK_OK);
    return 1;
}



static int zookeeper_getchilds (lua_State *L) {
    int index = 1;
    TO_ZKCLIENT(L,index); 

    assert( lua_isstring(L,index) );
    const char* path = lua_tostring(L,index++); 
    int sync = lua_toboolean(L,index++);

    int ret = zkclientGetChildrens(ZKCLIENT,path,__getChildrenNodeRTHandler,sync);
    lua_pushboolean(L,ret == ZK_OK);
    return 1;
}


static int zookeeper_existnode (lua_State *L) {
    int index = 1;
    TO_ZKCLIENT(L,index); 

    assert( lua_isstring(L,index) );
    const char* path = lua_tostring(L,index++); 
     int sync = lua_toboolean(L,index++);
    int ret = zkclientExistNode(ZKCLIENT, path,__existRTHandler,sync);
    lua_pushboolean(L,ret == ZK_OK);
    return 1;
}



static int zookeeper_nodewacher (lua_State *L) {
    int index = 1;
    TO_ZKCLIENT(L,index); 

    assert( lua_isstring(L,index) );
    const char* path = lua_tostring(L,index++); 

    int ret = zkclientNodeWacher(ZKCLIENT, path,__nodeEventHandler,0);
    lua_pushboolean(L,ret == ZK_OK);
    return 1;
}

static int zookeeper_childwacher (lua_State *L) {
    int index = 1;
    TO_ZKCLIENT(L,index); 

    assert( lua_isstring(L,index) );
    const char* path = lua_tostring(L,index++); 

    int ret = zkclientChildWacher(ZKCLIENT, path,__nodeEventHandler,0);
    lua_pushboolean(L,ret == ZK_OK);
    return 1;
}




//init func table
static const luaL_Reg zookeeperlib[] = {
    {"new",zookeeper_create},
    {"connect",zookeeper_connect},
    {"disconnect",zookeeper_disconnect},
    {"isconnect",zookeeper_isconnect},
    {"run",zookeeper_run},
    {"stop",zookeeper_stop},
    {"destory",zookeeper_destory},
    {"callback",zookeeper_callback},

    {"addauth",zookeeper_addauth},
    {"createnode",zookeeper_createnode},
    {"rcreatenode",zookeeper_recursive_createnode},
    {"setnode",zookeeper_setnode},
    {"getnode",zookeeper_getnode},
    {"delnode",zookeeper_delnode},
    {"getchilds",zookeeper_getchilds},
    {"existnode",zookeeper_existnode},

    {"nodewatcher",zookeeper_nodewacher},
    {"childwacher",zookeeper_childwacher},
    {NULL, NULL}
};


  
/*
** Open  library
*/
LUALIB_API int luaopen_zookeeper (lua_State *L) {
  luaL_newlib(L, zookeeperlib);  //C funcs to lua funcs
  return 1;
}