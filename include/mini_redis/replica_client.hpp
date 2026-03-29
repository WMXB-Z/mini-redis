#pragma once

#include <string>
#include <thread>

#include "mini_redis/config.hpp"

namespace mini_redis {
// 实现的是一个Redis从节点（Replica/Slave）客户端
/* 
1、启动一个后台线程
2、连接 Redis 主节点
3、发送同步请求（SYNC / PSYNC）
4、接收主节点数据：
    RDB 快照（全量同步）
    命令流（增量同步）
5、在本地执行这些数据 → 保持和主节点一致 */
class ReplicaClient {
public:
    explicit ReplicaClient(const ServerConfig &cfg);
    ~ReplicaClient();
    void start();
    void stop();

private:
    void threadMain();

private:
    const ServerConfig &cfg_;   //配置类
    std::thread th_;    //后台线程，用于连接主节点redis并完成数据同步的复制线程
    bool running_ = false;  //线程是否运行的标志
    int64_t last_offset_ = 0;   //记录当前已经同步到主节点数据的哪个位置
};

} // namespace mini_redis
