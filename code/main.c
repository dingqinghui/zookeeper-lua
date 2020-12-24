#include "lua.h"
#include "luaconf.h"
#include "lualib.h"
#include "lauxlib.h"

#include   <stdio.h>




int main(void)
{
   
  lua_State *L  = luaL_newstate();
  luaL_openlibs(L);
  int retCode = luaL_dofile(L,"../script/main.lua");
  if (retCode != 0)
  {
      printf("error %s\n",lua_tostring(L,-1));
      return;
  }


  lua_close(L);
  return 0;
}