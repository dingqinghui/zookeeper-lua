#include<stdio.h>
#include <stdlib.h>
#include <string.h>


#include <string.h>
#include <errno.h>
#include <stdarg.h>
#include <assert.h>
#include "zkcli.h"

//会话事件
#define SESSION_STATE_CLOSED     0
#define SESSION_STATE_CONNECTING 1
#define SESSION_STATE_CONNECTED  2  
#define SESSION_STATE_DISCONNECT 3  

#define MAX_PATH_DEEP    10
#define  MAX_PATH 256


#define  SESSION_DEF_TIMEOUT 10000

#define PRINTF printf

//权限生成
#define GEN_AUTH_ACL(auth) \
     struct ACL _CREATE_ONLY_ACL_ACL[] = {{ZOO_PERM_ALL, {"auth",auth}}};  \
     struct ACL_vector CREATE_ONLY_ACL = {1,_CREATE_ONLY_ACL_ACL};

#define ACL_VEC  CREATE_ONLY_ACL




#define CHECK_RC(rc,ctcx,str)  \
if(rc != ZOK){ \
    onReportErr("%s. rc:%s\n",str,zerror(rc));\
    if(ctcx){ \
       free(ctcx); \
    } \
    return -1;\
}

#define CHECK_CONNECTED(cli) \
if(!zkclientIsConnected(cli)){ \
  PRINTF("error zkclient state is %d",zkclientGetState(cli));\
  return -1; \
}



typedef struct zkclient{
    zhandle_t* zh;
    clientid_t myid;
    int stop;
    sessionConnectedHandler connectHandle;
    sessionCloseHandler closeHandle;
    int sessionState;
    int sessionExpireTimeout;
    int64_t connectingStartTime;
    int timeout;
    char* host;
    const char* auth;
    void* udata;
}zkclient;


//异步操作完成回调参数
typedef struct RtContext{
    zkclient* cli;
    void* context;
    char  path[MAX_PATH];
    union {
        createNodeRTHandler createRTHandler;
        setNodeRTHandler setRTHandler;
        getNodeRTHandler getRTHandler;
        deleteNodeRTHandler deleteRTHandler;
        getChildrenNodeRTHandler getChildrenRTHandler;
        existNodeRTHandler  existRTHandler;
        addAuthRTHandler authRTHandler;
    };
    nodeEventHandler wacher;
}RtContext;



typedef struct RTRecursiveCreate{
  int createNodeCnt ;
  char pathList[MAX_PATH_DEEP][MAX_PATH];
  zkclient* cli;
  createNodeRTHandler watcher;
  void* context;
  zoo_op_result_t resultList[MAX_PATH_DEEP];
  zoo_op_t ops[MAX_PATH_DEEP];
}RTRecursiveCreate;



static int strseppath(const char* path,char (*ipaths)[MAX_PATH],int ipathc);

static const char* onState2String(int state);
static const char* onType2String(int state);
static void onDumpStat(const struct Stat *stat);
static void onDumpStringVec(const struct String_vector *strings);
static int onReportErr(const char *fmt,...);
static int64_t OnGetCurrentMs() ;

static int onGetMode(int isTmp,int isSeq);
static void onSessionEventWacherFn(zhandle_t *zh, int type,int state, const char *path,void *watcherCtx);
static void onConnected(zkclient* cli);
static void onConnecting(zkclient* cli);
static int onDisconnect(zkclient* cli,int isExpire);
static int onSetState(zkclient* cli,int state);
static int onSelect(zkclient* cli);
static int onCheckSessionExpired(zkclient* cli);
static int onTriggerWacher(RtContext* rtCx,int event);

static int onWExist(zkclient* cli,RtContext* rtCx,char* path,watcher_fn fn);
static int onWGetChildrens(zkclient* cli,RtContext* rtCx,char* path,watcher_fn fn );

static void onGetChildrenCompletion(int rc, const struct String_vector *strings,const void *data) ;
static void onDeleteNodeCompletion(int rc, const void *data);
static void onGetDataCompletion(int rc, const char *value, int value_len,const struct Stat *stat, const void *data);
static void onSetDataCompletion(int rc, const struct Stat *stat,const void *data);
static void onCreateNodeCompletion(int rc, const char *value, const void *data) ;
static void onExistCompletion(int rc, const struct Stat *stat,const void *data);
static void onRecursiveCreateCompletion(int rc, const void *data);


static void onEventWatcher(zhandle_t *zh, int type,int state, const char *path,void *watcherCtx);
static int onGetEvent(RtContext* rtCx,int type);
static int onSubscribeEvent(RtContext* rtCx);




/////////////////////////////////////////////////////工具函数/////////////////////////////////////////////////////////////////////////

//会话状态
static const char* onState2String(int state){
  if (state == 0)
    return "CLOSED_STATE";
  if (state == ZOO_CONNECTING_STATE)
    return "CONNECTING_STATE";
  if (state == ZOO_ASSOCIATING_STATE)
    return "ASSOCIATING_STATE";
  if (state == ZOO_CONNECTED_STATE)
    return "CONNECTED_STATE";
  if (state == ZOO_READONLY_STATE)
    return "READONLY_STATE";
  if (state == ZOO_EXPIRED_SESSION_STATE)
    return "EXPIRED_SESSION_STATE";
  if (state == ZOO_AUTH_FAILED_STATE)
    return "AUTH_FAILED_STATE";

  return "INVALID_STATE";
}

//事件类型 会话/节点
static const char* onType2String(int state){
  if (state == ZOO_CREATED_EVENT)
    return "CREATED_EVENT";
  if (state == ZOO_DELETED_EVENT)
    return "DELETED_EVENT";
  if (state == ZOO_CHANGED_EVENT)
    return "CHANGED_EVENT";
  if (state == ZOO_CHILD_EVENT)
    return "CHILD_EVENT";
  if (state == ZOO_SESSION_EVENT)
    return "SESSION_EVENT";
  if (state == ZOO_NOTWATCHING_EVENT)
    return "NOTWATCHING_EVENT";

  return "UNKNOWN_EVENT_TYPE";
}

static const int onRc2Code(int rc){
   if(rc == ZOK){
      return ZKRT_SUCCESS;
    }
    else if(rc == ZNONODE){ 
      return ZKRT_NONODE;
    }
    else if(rc == ZNODEEXISTS){ 
      return ZKRT_NODEEXIST;
    }
    else if(rc == ZAUTHFAILED){ 
      return ZKRT_AUTHFAIL;
    }
    else{
      return ZKRT_ERROR;
    }
}

static void onDumpStat(const struct Stat *stat) {
    char tctimes[40];
    char tmtimes[40];
    time_t tctime;
    time_t tmtime;

    if (!stat) {
        PRINTF("null\n");
        return;
    }
    tctime = stat->ctime/1000;
    tmtime = stat->mtime/1000;

    ctime_r(&tmtime, tmtimes);
    ctime_r(&tctime, tctimes);

    PRINTF("\tctime = %s\tczxid=%llx\n"
    "\tmtime=%s\tmzxid=%llx\n"
    "\tversion=%x\taversion=%x\n"
    "\tephemeralOwner = %llx\n",
     tctimes, (long)stat->czxid, tmtimes,
    (long )stat->mzxid,
    (unsigned int)stat->version, (unsigned int)stat->aversion,
    (long) stat->ephemeralOwner);
}


static void onDumpStringVec(const struct String_vector *strings){
   if (strings){
      PRINTF("stringvec count:%d value:{",strings->count);
      for (int i=0; i < strings->count; i++) {
        PRINTF("\t%s\n", strings->data[i]);
      } 
      PRINTF("}\n");   
   }
   else{
     PRINTF("stringvec is null\n");
   }
}


static int onReportErr(const char *fmt,...){
    char* buf[256];

    va_list ap;
    va_start(ap, fmt);
    vsnprintf(buf, 256, fmt, ap);
    va_end(ap);

    PRINTF(buf);
}

static int onGetMode(int isTmp,int isSeq){
    if(isTmp){
      if(isSeq){
        return ZOO_EPHEMERAL_SEQUENTIAL;
      }
      else{
        return ZOO_EPHEMERAL;
      }
    }
    else{
      if(isSeq){
        return ZOO_PERSISTENT_SEQUENTIAL;
      }
      else{
        return ZOO_PERSISTENT;
      }
    }
}



static int64_t OnGetCurrentMs() {
	struct timeval tv;
	gettimeofday(&tv, NULL);
	return tv.tv_sec * 1000 + tv.tv_usec / 1000;
}
//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////


/////////////////////////////////////////////////////监控函数/////////////////////////////////////////////////////////////////////////
static void onSessionEventWacherFn(zhandle_t *zh, int type,int state, const char *path,void *watcherCtx){
    zkclient* cli = watcherCtx;
    PRINTF("trigger event type:%s state:%s \n",onType2String(type),onState2String(state));
    if(type == ZOO_SESSION_EVENT){
        if(state == ZOO_NOTCONNECTED_STATE){
          //初始阶段socket fd都没创建
          onDisconnect(cli,1);
        }
        else if(state == ZOO_CONNECTING_STATE){
          //TCP 三次握手中
          onConnecting(cli);
        }
        else if(state == ZOO_ASSOCIATING_STATE){
          //三次握手完成 未收到prime respond包(会话协商阶段)
          onConnecting(cli);
        }
        else if(state == ZOO_CONNECTED_STATE){    
          //建立会话
          onConnected(cli);
        }
        else if(state == ZOO_EXPIRED_SESSION_STATE){ // 
          //会话过期，不可恢复。被动关闭 回调逻辑层
          onDisconnect(cli,1);
        }
        else if(state == ZOO_AUTH_FAILED_STATE){
          //验证失败,不可恢复。被动关闭 回调逻辑层
          onDisconnect(cli,1);
        }
        else {
          PRINTF("invaild state:%s\n",onState2String(state));
        }
    }
}

//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////





zkclient* zkclientCreate(const char* host,sessionConnectedHandler connectHandle,sessionCloseHandler closeHandle,int timeout){
    zkclient* cli =  malloc(sizeof(zkclient));
    assert(cli != 0);
    cli->zh = 0;
    cli->stop = 0;
    cli->connectHandle = connectHandle;
    cli->closeHandle = closeHandle;
    cli->sessionState = SESSION_STATE_CLOSED;
    cli->sessionExpireTimeout = 0;
    cli->connectingStartTime = 0;
    cli->timeout = timeout > 0 ? timeout : SESSION_DEF_TIMEOUT;
    cli->host = strdup(host);
    cli->udata = 0;
    zoo_set_debug_level(ZOO_LOG_LEVEL_DEBUG);

    return cli;
}


void zkclientFree(zkclient* cli){
    assert(cli);
    zkclientDisconnect(cli);
    if(cli->host){
      free(cli->host);
    }
    if(cli->auth){
      free(cli->auth);
    }
    free(cli);
    cli = 0;
}

//建立会话
int zkclientConnect(zkclient* cli){
    assert(cli);
    zkclient* zh = zookeeper_init(cli->host, onSessionEventWacherFn, cli->timeout, 0, cli, 0);
    if (!zh) {
        return ZK_FAIL;
    }
    cli->zh = zh;
    return ZK_OK;
}

static void onConnected(zkclient* cli){
    assert(cli);
    onSetState(cli,SESSION_STATE_CONNECTED);

    int isReconnect = 1;
    const clientid_t * id = zoo_client_id(cli->zh);
    if(cli->myid.client_id == 0 ||  id->client_id != cli->myid.client_id){
        cli->myid = *id;
        isReconnect = 0;
    }
    if( cli->connectHandle ){
      cli->connectHandle(cli,isReconnect);
    }

    cli->sessionExpireTimeout = zoo_recv_timeout(cli->zh);
}

static void onConnecting(zkclient* cli){
    assert(cli);
    onSetState(cli,SESSION_STATE_CONNECTING);
    cli->connectingStartTime = OnGetCurrentMs();
}

static int onDisconnect(zkclient* cli,int isExpire){
    assert(cli);
    if(zkclientGetState(cli) == SESSION_STATE_CLOSED){
      return ZK_OK;
    }

    onSetState(cli,SESSION_STATE_CLOSED);

    if(cli->zh){
        zookeeper_close(cli->zh);
        cli->zh = 0;
    }
    if(cli->closeHandle){
      cli->closeHandle(cli,isExpire);
    }
    cli->sessionExpireTimeout = 0;
    cli->connectingStartTime  = 0;

    memset(&(cli->myid),0,sizeof(clientid_t));
    
    return ZK_OK;
}


int zkclientDisconnect(zkclient* cli){
    return onDisconnect(cli,0);
}




int zkclientRun(zkclient* cli){
    if(!cli->stop) {
      onSelect(cli) ;

      onCheckSessionExpired(cli);
    }
    return ZK_OK;
}

int zkclientStop(zkclient* cli){
    assert(cli);
    cli->stop = 1;
}

void zkclientSetUserData(zkclient* cli,void* udata){
    assert(cli);
    cli->udata = udata;
}

void* zkclientGetUserData(zkclient* cli){
    assert(cli);
    return cli->udata;
}

static int onSetState(zkclient* cli,int state){
   assert(cli != 0);
   cli->sessionState = state;
}

int zkclientGetState(zkclient* cli){
   assert(cli != 0);
   return cli->sessionState;
}

int zkclientIsConnected(zkclient* cli){
  return zkclientGetState(cli) == SESSION_STATE_CONNECTED;
}


// 发包+收包回+异步调用结果回调/监测函数回调
static int onSelect(zkclient* cli){
    assert(cli);
    zhandle_t* zh = cli->zh;
    fd_set rfds, wfds, efds;
    FD_ZERO(&rfds); FD_ZERO(&wfds);  FD_ZERO(&efds);
    
    int fd;
    int interest;
    int events;
    struct timeval tv = {0,0};
    int rc;
    zookeeper_interest(zh, &fd, &interest, &tv) ;

    if (fd != -1) {
        if (interest&ZOOKEEPER_READ) {
            FD_SET(fd, &rfds);
        } else {
            FD_CLR(fd, &rfds);
        }
        if (interest&ZOOKEEPER_WRITE) {
            FD_SET(fd, &wfds);
        } else {
            FD_CLR(fd, &wfds);
        }
    } else {
        fd = 0;
    }
    FD_SET(0, &rfds);
    rc = select(fd+1, &rfds, &wfds, &efds, &tv);
    events = 0;
    if (rc > 0) {
        if (FD_ISSET(fd, &rfds)) {
            events |= ZOOKEEPER_READ;
        }
        if (FD_ISSET(fd, &wfds)) {
            events |= ZOOKEEPER_WRITE;
        }
    }
      
   zookeeper_process(zh, events) ;
    return ZK_OK;
}
 
void onAddAuthCompletion(int rc, const void *data){
    RtContext* rtCx = data;
    assert(rtCx);

    int code = onRc2Code(rc);

    if(rtCx->authRTHandler){
       rtCx->authRTHandler(rtCx->cli,code,rtCx->context);
    }
    free(rtCx);
}


int zkclientAddAuth(zkclient* cli,const char* auth,addAuthRTHandler watcher,void* context){
    assert(cli);

    RtContext* rtCx = malloc(sizeof(RtContext));
    assert(rtCx);
    rtCx->cli = cli;
    rtCx->context = context;
    rtCx->createRTHandler = watcher;

    int rc = zoo_add_auth(cli->zh,"digest",auth,strlen(auth) + 1,onAddAuthCompletion,rtCx);
    CHECK_RC(rc,rtCx,"add auth")

    return 0;
}



static int strseppath(const char* path,char (*ipaths)[MAX_PATH],int ipathc){
    assert(path);
    assert(ipaths);
    int cnt = 0;
    char cpath[MAX_PATH]  = {'\0'};
    char *p;
    char *buff = strdup(path);
    p = strsep(&buff, "/");
    while(p!=NULL && ipathc > cnt)
    {
        p = strsep(&buff, "/");
        if(p!=NULL){
            if(strlen(p) > 0){
              sprintf(cpath,"%s/%s",cpath,p);
              memcpy(ipaths[cnt],cpath,(strlen(cpath) + 1)* sizeof(char) );
              cnt += 1;
            }
        }
    }
    free(buff);

    return cnt;
}




static void onRecursiveCreateCompletion(int rc, const void *data){
    RTRecursiveCreate* rtCx = data;
    assert(rtCx);

    //打印所有操作结果
    int code = rtCx->resultList[rtCx->createNodeCnt - 1].err == ZOK ? ZKRT_SUCCESS : ZKRT_ERROR;
    if(code == ZKRT_ERROR){
      for (size_t i = 0; i < rtCx->createNodeCnt; i++)
      {
          PRINTF("path:%s rc:%s\n",rtCx->pathList[i], zerror(rtCx->resultList[i].err) );
      }
    }

    if(rtCx->watcher){
       //回调
       rtCx->watcher(rtCx->cli,code,rtCx->pathList[rtCx->createNodeCnt - 1],0,rtCx->context);
    }
    free(rtCx);
}



//递归创建 isTmp isSeq 为叶子结点属性，父节点为永久非序列化节点
int zkclientRecursiveCreateNode(zkclient* cli,const char* path,const char* data,int len,int isTmp,int isSeq,createNodeRTHandler watcher,void* context,const char* auth){
    assert(cli);
    CHECK_CONNECTED(cli)

    RTRecursiveCreate* rtCx = malloc(sizeof(RTRecursiveCreate));
    rtCx->cli=cli;
    rtCx->watcher = watcher;
    rtCx->context = context;

    
    int cnt = strseppath(path,rtCx->pathList,MAX_PATH_DEEP);
    for (size_t i = 0; i < cnt; i++)
    {
      if(auth == 0){
        zoo_create_op_init((rtCx->ops + i), rtCx->pathList + i, data,
        len,  &ZOO_OPEN_ACL_UNSAFE, onGetMode( isTmp, isSeq),
        0, 0);
      }
      else{
        GEN_AUTH_ACL(cli->auth)
        zoo_create_op_init((rtCx->ops + i), rtCx->pathList + i, data,
        len,  &ACL_VEC, onGetMode( isTmp, isSeq),
        0, 0);
      }
    }

    if(cnt <= 0){
      free(rtCx);
      return -1;
    }

    rtCx->createNodeCnt = cnt;
    //ops中的操作执行失败后，后边的操作不会执行。  命令会回退。 类似于事务
    int rc = zoo_amulti(cli->zh, cnt, rtCx->ops,rtCx->resultList, onRecursiveCreateCompletion, rtCx);
    CHECK_RC(rc,rtCx,"recursive create node")

    return 0;
}



//创建节点
int zkclientCreateNode(zkclient* cli,const char* path,const char* data,int len,int isTmp,int isSeq,createNodeRTHandler watcher,void* context,const char* auth){
    assert(cli);

    CHECK_CONNECTED(cli)

    int mode = onGetMode( isTmp, isSeq);

    RtContext* rtCx = malloc(sizeof(RtContext));
    assert(rtCx);
    rtCx->cli = cli;
    memcpy(rtCx->path,path,(strlen(path) + 1) * sizeof(char) );
    rtCx->context = context;
    rtCx->createRTHandler = watcher;
    
    
    int rc = 0;
    if(auth == 0){
        rc = zoo_acreate(cli->zh,path,data,len,&ZOO_OPEN_ACL_UNSAFE,mode,onCreateNodeCompletion,rtCx);
    }
    else{
       GEN_AUTH_ACL(auth)
       rc = zoo_acreate(cli->zh,path,data,len,&ACL_VEC,mode,onCreateNodeCompletion,rtCx);
       
    }
    

    CHECK_RC(rc,rtCx,"create node")
   
    return 0;
 }

static void onCreateNodeCompletion(int rc, const char *value, const void *data) {
    RtContext* rtCx = data;
    assert(rtCx);
    int code = onRc2Code(rc);

    //PRINTF("create node:%s  data:%s result:%s\n",rtCx->path,value,zerror(rc));
    if(rtCx->createRTHandler){
       rtCx->createRTHandler(rtCx->cli,code,rtCx->path,value,rtCx->context);
    }

    free(rtCx);
}


//设置数据
 int zkclientSetNode(zkclient* cli,const char* path,const char* buff,int bufflen,setNodeRTHandler watcher,void* context){
    assert(cli);
    CHECK_CONNECTED(cli)

    RtContext* rtCx = malloc(sizeof(RtContext));
    assert(rtCx);
    rtCx->cli = cli;
    memcpy(rtCx->path,path,(strlen(path) + 1) * sizeof(char) );
    rtCx->context = context;
    rtCx->setRTHandler = watcher;

    int rc = zoo_aset(cli->zh,path,buff,bufflen,-1,onSetDataCompletion,rtCx);

    CHECK_RC(rc,rtCx,"set node")

  return 0;
 }



static  void onSetDataCompletion(int rc, const struct Stat *stat,const void *data)
{
    RtContext* rtCx = data;
    assert(rtCx);
    int code = onRc2Code(rc);
    ///PRINTF("set node:%s  result:%s\n",rtCx->path,zerror(rc));
    onDumpStat(stat);
    if(rtCx->setRTHandler){
       rtCx->setRTHandler(rtCx->cli,code,rtCx->path,stat,rtCx->context);
    }
    free(rtCx);
}

//获取数据
int zkclientGetNode(zkclient* cli,const char* path,getNodeRTHandler watcher,void* context){
    assert(cli);
    CHECK_CONNECTED(cli)

    RtContext* rtCx = malloc(sizeof(RtContext));
    assert(rtCx);
    rtCx->cli = cli;
    memcpy(rtCx->path,path,(strlen(path) + 1) * sizeof(char) );
    rtCx->context = context;
    rtCx->getRTHandler = watcher;

    int rc =  zoo_aget(cli->zh,path, 0,onGetDataCompletion, rtCx);
    CHECK_RC(rc,rtCx,"get node")
    return 0;
 }



static void onGetDataCompletion(int rc, const char *value, int value_len,const struct Stat *stat, const void *data)
{
    RtContext* rtCx = data;
    assert(rtCx);
    int code = onRc2Code(rc);
    //PRINTF("get node:%s value:%s result:%s\n",rtCx->path,value,zerror(rc));
    onDumpStat(stat);
    if(rtCx->getRTHandler){
       rtCx->getRTHandler(rtCx->cli,code,rtCx->path,value,value_len,stat,rtCx->context);
    }
    free(rtCx);
}



//删除节点
int zkclientDelNode(zkclient* cli,const char* path,deleteNodeRTHandler watcher,void* context){
    assert(cli);
    CHECK_CONNECTED(cli)

    RtContext* rtCx = malloc(sizeof(RtContext));
    assert(rtCx);
    rtCx->cli = cli;
    memcpy(rtCx->path,path,(strlen(path) + 1) * sizeof(char) );
    rtCx->context = context;
    rtCx->deleteRTHandler = watcher;

    int rc =  zoo_adelete(cli->zh,path, -1,onDeleteNodeCompletion, rtCx);
    CHECK_RC(rc,rtCx,"delete node")

    return 0;
}

static void onDeleteNodeCompletion(int rc, const void *data){
    RtContext* rtCx = data;
    assert(rtCx);
    int code = onRc2Code(rc);
    PRINTF("delete node:%s  result:%s\n",rtCx->path,zerror(rc));
    if(rtCx->deleteRTHandler){
       rtCx->deleteRTHandler(rtCx->cli,code,rtCx->path,rtCx->context);
    }
    free(rtCx);
}



//获取子节点列表
int zkclientGetChildrens(zkclient* cli,const char* path,getChildrenNodeRTHandler watcher,void* context){
    assert(cli);
    CHECK_CONNECTED(cli)

    RtContext* rtCx = malloc(sizeof(RtContext));
    assert(rtCx);
    rtCx->cli = cli;
    memcpy(rtCx->path,path,(strlen(path) + 1) * sizeof(char) );
    rtCx->context = context;
    rtCx->getChildrenRTHandler = watcher;

    return onWGetChildrens(cli, rtCx,path,0);
}

static int onWGetChildrens(zkclient* cli,RtContext* rtCx,char* path,watcher_fn fn ){
    int rc= zoo_awget_children(cli->zh, path, fn, rtCx, onGetChildrenCompletion, rtCx);
    CHECK_RC(rc,rtCx,"zoo_aget_children")
    return 0;
}



static void onGetChildrenCompletion(int rc, const struct String_vector *strings,const void *data) {
    RtContext* rtCx = data;
    assert(rtCx);
    int code = onRc2Code(rc);
    //PRINTF("get childrens node:%s  result:%s\n",rtCx->path,zerror(rc));
    onDumpStringVec(strings);
    if(rtCx->getChildrenRTHandler){
       rtCx->getChildrenRTHandler(rtCx->cli,code,rtCx->path,strings,rtCx->context);
    }
    if(rc != ZOK){
      onTriggerWacher(rtCx,EventWacherFail);
      free(rtCx);
      PRINTF("get child node:%s  result:%s\n",rtCx->path,zerror(rc));
    }
    else{
      rtCx->wacher == 0 ? free(rtCx) : 0 ;
    }
}

//节点是否存在
int  zkclientExistNode(zkclient* cli, const char* path,existNodeRTHandler watcher,void* context){
    assert(cli);
    CHECK_CONNECTED(cli)

    RtContext* rtCx = malloc(sizeof(RtContext));
    assert(rtCx);
    rtCx->cli = cli;
    memcpy(rtCx->path,path,(strlen(path) + 1) * sizeof(char) );
    rtCx->context = context;
    rtCx->existRTHandler = watcher;

    return onWExist(rtCx->cli, rtCx,rtCx->path,0);
}

static int onWExist(zkclient* cli,RtContext* rtCx,char* path,watcher_fn fn){
    
    int rc= zoo_awexists(cli->zh, rtCx->path, fn, rtCx,onExistCompletion, rtCx);
    CHECK_RC(rc,rtCx,"zoo_awexists ")
    return 0;
}


static void onExistCompletion(int rc, const struct Stat *stat,const void *data){
    RtContext* rtCx = data;
    assert(rtCx);
    
    //onDumpStat(stat);

    int code = onRc2Code(rc);

    if(rtCx->existRTHandler){
       rtCx->existRTHandler(rtCx->cli,code,rtCx->path,stat,rtCx->context);
    }
    if(rc != ZOK && rc != ZNONODE){
      onTriggerWacher(rtCx,EventWacherFail);
      free(rtCx);
      PRINTF("exist node:%s  result:%s\n",rtCx->path,zerror(rc));
    }
    else{
      rtCx->wacher == 0 ? free(rtCx) : 0 ;
    }

}






int zkclientNodeWacher(zkclient* cli,const char* path,nodeEventHandler wacher,void* context){
    assert(cli);
    CHECK_CONNECTED(cli)

    if(!wacher){
      return 0;
    }
    RtContext* rtCx = malloc(sizeof(RtContext));
    assert(rtCx);
    
    rtCx->cli = cli;
    memcpy(rtCx->path,path,(strlen(path) + 1) * sizeof(char) );
    rtCx->context = context;
    rtCx->wacher = wacher;
    rtCx->existRTHandler = 0;

    //PRINTF("zkclientNodeWacher  rtCx:%x path:%s  \n",rtCx,rtCx->path);

    return onWExist(rtCx->cli, rtCx,rtCx->path,onEventWatcher);
}


int zkclientChildWacher(zkclient* cli,const char* path,nodeEventHandler wacher,void* context){
    assert(cli);
    CHECK_CONNECTED(cli)

    if(!wacher){
      return 0;
    }
    RtContext* rtCx = malloc(sizeof(RtContext));
    assert(rtCx);
    
    rtCx->cli = cli;
    memcpy(rtCx->path,path,(strlen(path) + 1) * sizeof(char) );
    rtCx->context = context;
    rtCx->wacher = wacher;
    rtCx->getChildrenRTHandler = 0;

    //PRINTF("zkclientChildWacher  rtCx:%x path:%s  \n",rtCx,rtCx->path);

    return onWGetChildrens(rtCx->cli, rtCx,rtCx->path,onEventWatcher);
}

static int onGetEvent(RtContext* rtCx,int type){ 
     if ( type == ZOO_CREATED_EVENT){
      return EventNodeCreated;
     }
     else if ( type == ZOO_DELETED_EVENT){
      return EventNodeDeleted;
     }
    else if ( type == ZOO_CHANGED_EVENT){
     return EventNodeDataChanged;
     }
     
    else if ( type == ZOO_CHILD_EVENT){
      return EventNodeChildrenChanged;
    }
    return EventWacherFail;
}

static void onEventWatcher(zhandle_t *zh, int type,int state, const char *path,void *watcherCtx){
    if( type == ZOO_SESSION_EVENT){
      return ;
    }

    RtContext* rtCx = watcherCtx;
    int event = onGetEvent(rtCx,type);
    if( event >= 0 )
    {
        onTriggerWacher(rtCx,event);
        free(rtCx);
    } 
    else{
      if( type == ZOO_NOTWATCHING_EVENT ){
         onTriggerWacher(rtCx,EventWacherFail);
         free(rtCx);
      }
    }
 }



static int onTriggerWacher(RtContext* rtCx,int event){
   if(rtCx->wacher){
      rtCx->wacher(rtCx->cli, event,rtCx->path,rtCx->context);
    }
    rtCx = 0;
}

//主动检测会话超时
static int onCheckSessionExpired(zkclient* cli){
  assert(cli);
  if(cli->sessionExpireTimeout != 0){
      if( zkclientGetState(cli) == SESSION_STATE_CONNECTING){
        if(OnGetCurrentMs() - cli->connectingStartTime >  cli->sessionExpireTimeout){
          onDisconnect(cli,1);
        }
      }
   }
   return ZK_OK;
}


const char* Event2String(int event){
    if ( event == EventNodeCreated){
      return "EventNodeCreated";
    }
    else if ( event == EventNodeDeleted){
      return "EventNodeDeleted";
    }
    else if ( event == EventNodeDataChanged){
      return "EventNodeDataChanged";
    }
    else if ( event == EventNodeChildrenChanged){
      return "EventNodeChildrenChanged";
    }
    else if( event == EventWacherFail){
      return "EventWacherFail"; 
    }
    else{
      return "unknown event";
    }
}