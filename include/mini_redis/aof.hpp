#pragma once
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <deque>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <vector>

#include "mini_redis/config.hpp"

namespace mini_redis {

class KeyValueStore;
// AOF（Append Only File）持久化模块的主要作用
// 记录写命令（append）
// 恢复数据（load/replay）
// 压缩日志（BGREWRITEAOF）

class AofLogger {
public:
    AofLogger();
    ~AofLogger();

    bool init(const AofOptions &opts, std::string &err);
    void shutdown();

    //从现存的AOF文件中读取已有命令并执行，即实现“恢复数据库”
    bool load(KeyValueStore &store, std::string &err);

    //向 AOF（Append-Only File）追加写入一条命令
    bool appendCommand(const std::vector<std::string> &parts);
    bool appendRaw(const std::string &raw_resp);

    bool isEnabled() const { 
        return opts_.enabled; 
    }
    AofMode mode() const { 
        return opts_.mode; 
    }
    std::string path() const;
    bool bgRewrite(KeyValueStore &store, std::string &err);

private:
    int fd_ = -1;           //aof文件的文件描述符
    AofOptions opts_;
    std::atomic<bool> running_{false};
    // int timer_fd_ = -1;

    struct AofItem {
        std::string data;
        int64_t seq;
    };

    std::thread writer_thread_;        //后台的追写AOF线程
    std::mutex mtx_;                   //通用的互斥信号两
    std::condition_variable cv_;       //queue_队列为空时阻塞后追写线程，aof写线程终止或queue_非空时由主线程唤醒
    std::condition_variable cv_commit_; //持久化模型为kalways时，阻塞主进程，追写线程完成落盘后唤醒

    std::deque<AofItem> queue_;     //AOF写入缓存队列，存放还没写入AOF文件的命令
    std::atomic<bool> stop_{false}; //写入线程是否需要停止
    size_t pending_bytes_ = 0;      //队列中还没写入磁盘的字节数
    std::chrono::steady_clock::time_point last_sync_tp_{ std::chrono::steady_clock::now()}; //上一次执行fdatasync的时间点
    std::atomic<int64_t> seq_gen_{0};   //全局命令序号生成器,每次生成一个新的AofItem 时，seq_gen_++
    int64_t last_synced_seq_ = 0;       //最后已经安全写入磁盘的序号

    // AOF文件压缩重写状态标志
    std::atomic<bool> rewriting_{false};    //是否正在执行重写操作，这是为了控制重写线程只有单线程
    std::thread rewriter_thread_;       //后台的重写AOF线程
    std::mutex incr_mtx_;               //用于对incr_cmds_访问使的互斥信号
    std::vector<std::string> incr_cmds_;//AOF重写缓冲队列，存放还没写入AOF重写文件的命令

    // 暂停写入线程
    std::atomic<bool> pause_writer_{false}; //追写线程暂停请求标志，表示是否请求暂停（用于AOF的重写时，追写AOF线程的阻塞）
    std::mutex pause_mtx_;
    std::condition_variable cv_pause_;      //重写线程执行前，需要先阻塞追写线程；追写线程阻塞后，重写线程才被唤醒（有点像同步屏障）
    bool writer_is_paused_ = false;         //追写线程暂停标志，AOF后台写进程是否已经暂停了，用作唤醒重写线程的条件

    void writerLoop();
    void rewriterLoop(KeyValueStore *store);
};

// helpers
std::string respArray(const std::vector<std::string> &parts);

} // namespace mini_redis
