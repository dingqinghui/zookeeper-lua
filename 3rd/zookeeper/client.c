#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <unistd.h>
#include "zkcli.h"



void getChildrenRT(zkclient* cli,int errCode,const char* path,const struct String_vector *strings,void* context){
    switch(errCode){
        case ZKRT_SUCCESS:{
            printf("get child node data completion path[%s] value[%s] .\n",path);
            char childPath [256];
            for(int i = 0;i < strings->count;++i){
                sprintf(childPath,"%s/%s",path,strings->data[i]);

                printf("childnode path:%s\n",childPath);
            }
        }
        break;
        case ZKRT_NONODE:
            printf("get child node data completion path[%s] not exsist.\n",path);
        break;
        case ZKRT_ERROR:
            
        break;
    }
    
}

void getNodeRT(zkclient* cli,int errCode,const char* path,const char* buff,int bufflen,const struct Stat *stat,void* context){
    switch(errCode){
        case ZKRT_SUCCESS:
            printf("get node data completion path[%s] value[%s] .\n",path,buff);
        break;
        case ZKRT_NONODE:
            printf("get node data completion path[%s] not exsist.\n",path);
        break;
        case ZKRT_ERROR:
            
        break;
    }
}

void existNodeRT(zkclient* cli, int errCode,const char* path,const struct Stat *stat,const void *data){
    switch(errCode){
        case ZKRT_SUCCESS:
            printf("exist node completion path[%s] exsist.\n",path);
        break;
        case ZKRT_NONODE:
            printf("exist node completion path[%s] not exsist.\n",path);
        break;
        case ZKRT_ERROR:
            
        break;
    }
}


void nodeWacher(zkclient* cli,int event,const char* path,void* context){
    printf("wacher event[%s] path[%s]\n",Event2String(event),path);
    //再次获取节点最新状态状态
    switch(event){
        case EventNodeCreated:
        {
            zkclientExistNode(cli,path,existNodeRT,cli);
            zkclientNodeWacher(cli,path,nodeWacher,cli);
        }
          
        break;
        case EventNodeDeleted:
        {
            zkclientExistNode(cli,path,existNodeRT,cli);
            zkclientNodeWacher(cli,path,nodeWacher,cli);
        }
           
        break;
        case EventNodeDataChanged:
        {
            zkclientExistNode(cli,path,getNodeRT,cli);
            zkclientNodeWacher(cli,path,nodeWacher,cli);
        } 
        break;
        case EventNodeChildrenChanged:
        {
            zkclientGetChildrens(cli,path,getChildrenRT,cli);
            zkclientChildWacher(cli,path,nodeWacher,cli);
        }
        break;
        case EventWacherFail:
            
        break;
    }
}





 void ConnectedHandler(zkclient* cli,int isReconnect){
    printf("ConnectedHandler \n");
    if(isReconnect){
        return ;
    }
    
    //监控节点
    //zkclientNodeWacher(cli,"/parent",nodeWacher,cli);
    //zkclientChildWacher(cli,"/parent",nodeWacher,cli);  //parent不存在则会报错
    zkclientAddAuth(cli,"dingqinghui:dingqinghui",0,0);
    zkclientRecursiveCreateNode(cli,"/parent111","child",5,1,0,0,0,"dingqinghui:dingqinghui");
    //zkclientDelNode(cli,"/parent111",0,0);
   
 }


 
 void CloseHandler(zkclient* cli,int isExpire){
    printf("CloseHandler \n");
    if(isExpire){
        zkclientConnect(cli);
    }
 }






int main(int argc,const char*argv[])
{

    char*host="127.0.0.1:2183,127.0.0.1:2184,127.0.0.1:2185";

    zkclient* zkcli  = zkclientCreate(host,ConnectedHandler,CloseHandler,0);
    assert(zkcli);

    zkclientConnect(zkcli);

    

    while(1){
        zkclientRun(zkcli);
    }

    zkclientFree(zkcli);

    return 0;
}
     