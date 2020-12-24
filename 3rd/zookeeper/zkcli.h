/////////////////////单线程异步客户端///////////////////////////////////
#ifndef __ZKCLIENT_H__
#define __ZKCLIENT_H__

#include <zookeeper.h>
#include <zookeeper_log.h>

typedef struct zkclient zkclient;


#define ZK_FAIL     -1
#define ZK_OK        0


//异步回调错误码
#define ZKRT_ERROR      -1
#define ZKRT_SUCCESS    0
#define ZKRT_NONODE     1    //节点/父节点不存在
#define ZKRT_NODEEXIST  2    //节点存在
#define ZKRT_AUTHFAIL   3    //添加用户失败


//节点事件
#define EventWacherFail             -1 //注册事件失败
#define EventNodeCreated             0
#define EventNodeDeleted             1
#define EventNodeDataChanged         2
#define EventNodeChildrenChanged     3



//会话事件回调
typedef void (*sessionConnectedHandler)(zkclient* cli,int isReconnect);
typedef void (*sessionCloseHandler)(zkclient* cli,int isExpire);


//监测点事件回调
typedef void (*nodeEventHandler)(zkclient* cli,int eventType,const char* path,void* context);


//异步调用结果回调
typedef void (*createNodeRTHandler)(zkclient* cli,int errCode,const char* path,const char* value,void* context);
typedef void (*setNodeRTHandler)(zkclient* cli,int errCode,const char* path,const struct Stat *stat,void* context);
typedef void (*getNodeRTHandler)(zkclient* cli,int errCode,const char* path,const char* buff,int bufflen,const struct Stat *stat,void* context);
typedef void (*deleteNodeRTHandler)(zkclient* cli,int errCode,const char* path,void* context);
typedef void (*getChildrenNodeRTHandler)(zkclient* cli,int errCode,const char* path,const struct String_vector *strings,void* context);
typedef void (*existNodeRTHandler)(zkclient* cli, int errCode,const char* path,const struct Stat *stat,const void *data);
typedef void (*addAuthRTHandler)(zkclient* cli,int errCode,void* context);



zkclient* zkclientCreate(const char* host, sessionConnectedHandler connectHandle,sessionCloseHandler closeHandle,int timeout);
void zkclientFree(zkclient* cli);

int zkclientConnect(zkclient* cli);
int zkclientDisconnect(zkclient* cli);

int zkclientIsConnected(zkclient* cli);

int zkclientRun(zkclient* cli);
int zkclientStop(zkclient* cli);

void zkclientSetUserData(zkclient* cli,void* udata);
void* zkclientGetUserData(zkclient* cli);

int zkclientGetState(zkclient* cli);

int zkclientAddAuth(zkclient* cli,const char* auth,addAuthRTHandler watcher,void* context);

int zkclientCreateNode(zkclient* cli,const char* path,const char* data,int len,int isTmp,int isSeq,createNodeRTHandler watcher,void* context,const char* auth);
//递归创建 isTmp isSeq 为叶子结点属性，父节点为永久非序列化节点
int zkclientRecursiveCreateNode(zkclient* cli,const char* path,const char* data,int len,int isTmp,int isSeq,createNodeRTHandler watcher,void* context,const char* auth);
int zkclientSetNode(zkclient* cli,const char* path,const char* buff,int bufflen,setNodeRTHandler watcher,void* context);
int zkclientGetNode(zkclient* cli,const char* path,getNodeRTHandler watcher,void* context);
int zkclientDelNode(zkclient* cli,const char* path,deleteNodeRTHandler watcher,void* context);
int zkclientGetChildrens(zkclient* cli,const char* path,getChildrenNodeRTHandler watcher,void* context);
int zkclientExistNode(zkclient* cli, const char* path,existNodeRTHandler watcher,void* context);
int zkclientNodeWacher(zkclient* cli,const char* path,nodeEventHandler wacher,void* context);
int zkclientChildWacher(zkclient* cli,const char* path,nodeEventHandler wacher,void* context);

const char* Event2String(int event);

#endif