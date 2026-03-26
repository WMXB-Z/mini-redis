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

class AofLogger {
public:
    AofLogger();
    ~AofLogger();

    bool init(const AofOptions &opts, std::string &err);
    void shutdown();

    // Replay existing AOF into store. Returns true on success.
    bool load(KeyValueStore &store, std::string &err);

    // Append a RESP array command like {"SET","k","v"}
    bool appendCommand(const std::vector<std::string> &parts);

    // Append raw RESP command bytes directly (as received), avoiding
    // re-serialization
    bool appendRaw(const std::string &raw_resp);

    bool isEnabled() const { return opts_.enabled; }
    AofMode mode() const { return opts_.mode; }
    std::string path() const;

    // Trigger background AOF rewrite (BGREWRITEAOF). Returns false if already
    // running or AOF disabled.
    bool bgRewrite(KeyValueStore &store, std::string &err);

private:
    int fd_ = -1;
    AofOptions opts_;
    std::atomic<bool> running_{false};
    int timer_fd_ = -1; // used for everysec fsync

    // async writer
    struct AofItem {
        std::string data;
        int64_t seq;
    };
    std::thread writer_thread_;
    std::mutex mtx_;
    std::condition_variable cv_;
    std::condition_variable cv_commit_;
    std::deque<AofItem> queue_;
    std::atomic<bool> stop_{false};
    size_t pending_bytes_ = 0;
    std::chrono::steady_clock::time_point last_sync_tp_{
        std::chrono::steady_clock::now()};
    std::atomic<int64_t> seq_gen_{0};
    int64_t last_synced_seq_ = 0;

    // BGREWRITEAOF state
    std::atomic<bool> rewriting_{false};
    std::thread rewriter_thread_;
    std::mutex incr_mtx_;
    std::vector<std::string> incr_cmds_;

    // Pause writer to safely swap file descriptors
    std::atomic<bool> pause_writer_{false};
    std::mutex pause_mtx_;
    std::condition_variable cv_pause_;
    bool writer_is_paused_ = false;

    void writerLoop();
    void rewriterLoop(KeyValueStore *store);
};

// helpers
std::string toRespArray(const std::vector<std::string> &parts);

} // namespace mini_redis
