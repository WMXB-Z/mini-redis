#pragma once

#include <string>

namespace mini_redis {

enum class AofMode { kNo, kEverySec, kAlways };
// AOF的配置类
struct AofOptions {
    bool enabled = false;                //AOF功能是否开启
    AofMode mode = AofMode::kEverySec;  //aof的同步模式，这里默认是每秒同步一次
    std::string dir = "./data";          //AOF重写文件的所在目录
    std::string filename = "appendonly.aof";    ////AOF重写文件的文件名
    // ===================aof中的可调参数==============================
    size_t batch_bytes = 256 * 1024;          // AOF文件写入时，单次最多写多少字节
    int batch_wait_us = 1500;                 // // 如果队列为空，最多等待多久
    size_t prealloc_bytes = 64 * 1024 * 1024; // LInux上给文件AOF文件初始预分配的大小
    int sync_interval_ms = 1000;              // everysec 实际同步周期（毫秒）
    bool use_sync_file_range = false;         // 用户是否启用落盘优化，即写入aof后触发后台回写（SFR_WRITE）
    size_t sfr_min_bytes = 512 * 1024;        // 触发主动写回的数据阈值，即写入aif的数据量到达这个数量后，才触发主动写回
    bool fadvise_dontneed_after_sync = false; // 用户是否启用缓存清除优化，每次fdatasync 后对已同步范围做 DONTNEED
};

// RDB的配置类
struct RdbOptions {
    bool enabled = true;
    std::string dir = "./data";
    std::string filename = "dump.rdb";
};

// 主从复制配置类
struct ReplicaOptions {
    bool enabled = false;
    std::string master_host = "";
    uint16_t master_port = 0;
};

// redis服务整体配置类
struct ServerConfig {
    uint16_t port = 6379;
    std::string bind_address = "0.0.0.0";
    AofOptions aof;
    RdbOptions rdb;
    ReplicaOptions replica;
};

} // namespace mini_redis
