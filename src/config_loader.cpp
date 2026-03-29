#include "mini_redis/config_loader.hpp"
#include <cctype>
#include <fstream>
#include <sstream>

#include "mini_redis/config.hpp"

namespace mini_redis {

// 去除字符串首尾的空白字符
static std::string trim(const std::string &s) {
    size_t i = 0, j = s.size();
    while (i < j && std::isspace(static_cast<unsigned char>(s[i])))
        ++i;
    while (j > i && std::isspace(static_cast<unsigned char>(s[j - 1])))
        --j;
    return s.substr(i, j - i);
}

/* 配置文件的解析函数，实现对配置文件中信息的解析
含义：
path：配置文件路径
cfg：输出参数，存储解析结果
err：错误信息
返回：
true：成功
false：失败（同时设置 err） */
bool loadConfigFromFile(const std::string &path, ServerConfig &cfg, std::string &err) {
    std::ifstream in(path); //用ifstream打开文件
    if (!in.good()) {   //in.good()判断是否成功
        err = "open config failed: " + path;
        return false;
    }

    std::string line;
    int lineno = 0;
    while (std::getline(in, line)) {
        ++lineno;
        std::string t = trim(line);
        if (t.empty() || t[0] == '#')   //#表示这一行是注释
            continue;

        auto pos = t.find('=');
        if (pos == std::string::npos) { //如果没有找到=，说明这行格式有问题，报错
            err = "invalid line " + std::to_string(lineno);
            return false;
        }
        std::string key = trim(t.substr(0, pos));   //取得'='前的字符串，作为key
        std::string val = trim(t.substr(pos + 1));  //取得'='后的字符串，作为val
        if (key == "port") {    //设置服务器监听的端口号
            try {
                cfg.port = static_cast<uint16_t>(std::stoi(val));
            } catch (...) {
                err = "invalid port at line " + std::to_string(lineno);
                return false;
            }
        } else if (key == "bind_address") {//设置服务器的IP
            cfg.bind_address = val;
        } else if (key == "aof.enabled") {//设置是否开启 AOF
            cfg.aof.enabled = (val == "1" || val == "true" || val == "yes");
        } else if (key == "aof.mode") {//设置AOF的写盘策略
            // AOF 写盘策略（核心参数！）
            // 值	含义
            // no	不主动 fsync（依赖系统）
            // everysec	每秒同步一次（推荐）
            // always	每次写都同步（最安全但最慢）
            if (val == "no")
                cfg.aof.mode = AofMode::kNo;
            else if (val == "everysec")
                cfg.aof.mode = AofMode::kEverySec;
            else if (val == "always")
                cfg.aof.mode = AofMode::kAlways;
            else {
                err = "invalid aof.mode at line " + std::to_string(lineno);
                return false;
            }
        } else if (key == "aof.dir") {//设置AOF文件存储目录
            cfg.aof.dir = val;
        } else if (key == "aof.filename") {//设置AOF文件名
            cfg.aof.filename = val;
        } else if (key == "aof.batch_bytes") {//设置写入缓冲区累计多少字节后再批量写，批量写（batching）优化，减少系统调用次数（write）
            try {
                cfg.aof.batch_bytes = static_cast<size_t>(std::stoull(val));
            } catch (...) {
                err =
                    "invalid aof.batch_bytes at line " + std::to_string(lineno);
                return false;
            }
        } else if (key == "aof.batch_wait_us") {//设置等待更多数据聚合的时间大小（微秒），减少小包的写入，用“时间换吞吐量”  
            try {
                cfg.aof.batch_wait_us = std::stoi(val);
            } catch (...) {
                err = "invalid aof.batch_wait_us at line " +
                      std::to_string(lineno);
                return false;
            }
        } else if (key == "aof.prealloc_bytes") {//设置预分配文件空间大小，预分配文件空间，避免频繁扩容，提高磁盘写性能
            try {
                cfg.aof.prealloc_bytes = static_cast<size_t>(std::stoull(val));
            } catch (...) {
                err = "invalid aof.prealloc_bytes at line " +
                      std::to_string(lineno);
                return false;
            }
        } else if (key == "aof.sync_interval_ms") {//设置fsync的周期（毫秒），对应everysec模式
            try {
                cfg.aof.sync_interval_ms = std::stoi(val);
            } catch (...) {
                err = "invalid aof.sync_interval_ms at line " +
                      std::to_string(lineno);
                return false;
            }
        } else if (key == "aof.use_sync_file_range") {//设置是否使用 sync_file_range，更细粒度控制磁盘刷写（比 fsync 更轻量）
            cfg.aof.use_sync_file_range =
                (val == "1" || val == "true" || val == "yes");
        } else if (key == "aof.sfr_min_bytes") {//设置达到多少字节才调用sync_file_range
            try {
                cfg.aof.sfr_min_bytes = static_cast<size_t>(std::stoull(val));
            } catch (...) {
                err = "invalid aof.sfr_min_bytes at line " +
                      std::to_string(lineno);
                return false;
            }
        } else if (key == "aof.fadvise_dontneed_after_sync") {//设置写完后是否调用posix_fadvise(DONTNEED)。释放 page cache，避免缓存污染
            cfg.aof.fadvise_dontneed_after_sync = (val == "1" || val == "true" || val == "yes");
        } else if (key == "rdb.enabled") {//设置是否启用 RDB（另一种持久化方式）
            cfg.rdb.enabled = (val == "1" || val == "true" || val == "yes");
        } else if (key == "rdb.dir") {//设置快照保存目录
            cfg.rdb.dir = val;
        } else if (key == "rdb.filename") {//设置快照文件名
            cfg.rdb.filename = val;
        } else if (key == "replica.enabled") { //设置是否作为从节点redis
            cfg.replica.enabled = (val == "1" || val == "true" || val == "yes");
        } else if (key == "replica.master_host") {//设置主节点的IP地址
            cfg.replica.master_host = val;
        } else if (key == "replica.master_port") {//设置主节点的端口号
            try {
                cfg.replica.master_port = static_cast<uint16_t>(std::stoi(val));
            } catch (...) {
                err = "invalid replica.master_port at line " +
                      std::to_string(lineno);
                return false;
            }
        } else {
            // ignore unknown keys for forward compatibility
        }
    }
    return true;
}

} // namespace mini_redis
