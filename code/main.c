#include "lua.h"
#include "luaconf.h"
#include "lualib.h"
#include "lauxlib.h"

#include   <stdio.h>

void* __alloc(void *ud, void *ptr, size_t osize, size_t nsize){
    lua_State *L = ud;
    return realloc(ptr, nsize);
}


int main(void)
{
  char buf[80];   
  getcwd(buf,sizeof(buf));   
  printf("current working directory: %s\n", buf);   

  lua_State *L  = lua_newstate(__alloc,L);
  luaL_openlibs(L);

  int retCode = luaL_dofile(L,"./script/main.lua");
  if (retCode != 0)
  {
      printf("error %s\n",lua_tostring(L,-1));
      return;
  }
  

  lua_close(L);
  return 0;
}