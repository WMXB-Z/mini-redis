#pragma once

namespace mini_redis {

class KeyValueStore;    //前向声明

extern KeyValueStore g_store;   //表明g_store是一个外部变量，具体的定义在别的cpp中

}  // namespace mini_redis


