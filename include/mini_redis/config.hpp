#pragma once

#include <string>

namespace mini_redis {

enum class AofMode { kNo, kEverySec, kAlways };

struct AofOptions {
    bool enabled = false;
    AofMode mode = AofMode::kEverySec;
    std::string dir = "./data";
    std::string filename = "appendonly.aof";
    // tunables
    size_t batch_bytes = 256 * 1024;          // 每批聚合写入的目标字节数
    int batch_wait_us = 1500;                 // 聚合等待上限（微秒）
    size_t prealloc_bytes = 64 * 1024 * 1024; // 初始预分配大小
    int sync_interval_ms =
        1000; // everysec 实际同步周期（毫秒），可调平滑尾延迟
    // optional smoothing knobs (Linux only)
    bool use_sync_file_range = false; // 写入后触发后台回写（SFR_WRITE）
    size_t sfr_min_bytes =
        512 * 1024; // 达到该批量再调用 sync_file_range，避免过于频繁
    bool fadvise_dontneed_after_sync =
        false; // 每次 fdatasync 后对已同步范围做 DONTNEED
};

struct RdbOptions {
    bool enabled = true;
    std::string dir = "./data";
    std::string filename = "dump.rdb";
};

struct ReplicaOptions {
    bool enabled = false;
    std::string master_host = "";
    uint16_t master_port = 0;
};

struct ServerConfig {
    uint16_t port = 6379;
    std::string bind_address = "0.0.0.0";
    AofOptions aof;
    RdbOptions rdb;
    ReplicaOptions replica;
};

} // namespace mini_redis
