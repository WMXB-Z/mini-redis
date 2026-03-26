#include "mini_redis/aof.hpp"

#include "mini_redis/kv.hpp"
#include "mini_redis/log.hpp"

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <errno.h>
#include <filesystem>
#include <optional>
#include <sys/stat.h>
#include <sys/uio.h>

namespace mini_redis {

static std::string joinPath(const std::string &dir, const std::string &file) {
    if (dir.empty())
        return file;
    if (dir.back() == '/')
        return dir + file;
    return dir + "/" + file;
}

static bool writeAllFD(int fd, const char *data, size_t len) {
    size_t off = 0;
    while (off < len) {
        ssize_t w = ::write(fd, data + off, len - off);
        if (w > 0) {
            off += static_cast<size_t>(w);
            continue;
        }
        if (w < 0 && (errno == EINTR || errno == EAGAIN)) {
            continue;
        }
        return false;
    }
    return true;
}


std::string toRespArray(const std::vector<std::string> &parts) {
    std::string out;
    out.reserve(16 * parts.size());
    out.append("*").append(std::to_string(parts.size())).append("\r\n");
    for (const auto &p : parts) {
        out.append("$").append(std::to_string(p.size())).append("\r\n");
        out.append(p).append("\r\n");
    }
    return out;
}

std::optional<AofMode> parseAofMode(const std::string &s) {
    if (s == "no")
        return AofMode::kNo;
    if (s == "everysec")
        return AofMode::kEverySec;
    if (s == "always")
        return AofMode::kAlways;
    return std::nullopt;
}

AofLogger::AofLogger() = default;
AofLogger::~AofLogger() { shutdown(); }

std::string AofLogger::path() const {
    return joinPath(opts_.dir, opts_.filename);
}

bool AofLogger::init(const AofOptions &opts, std::string &err) {
    opts_ = opts;
    if (!opts_.enabled)
        return true;
    std::error_code ec;
    std::filesystem::create_directories(opts_.dir, ec);
    if (ec) {
        err = "mkdir failed: " + opts_.dir;
        return false;
    }
    fd_ = ::open(path().c_str(), O_CREAT | O_APPEND | O_WRONLY, 0644);
    if (fd_ < 0) {
        err = "open AOF failed: " + path();
        return false;
    }
    // 预分配，降低元数据更新成本
#ifdef __linux__
    if (opts_.prealloc_bytes > 0) {
        posix_fallocate(fd_, 0, static_cast<off_t>(opts_.prealloc_bytes));
    }
#endif
    running_.store(true);
    stop_.store(false);
    writer_thread_ = std::thread(&AofLogger::writerLoop, this);
    return true;
}

void AofLogger::shutdown() {
    running_.store(false);
    stop_.store(true);
    cv_.notify_all();
    if (writer_thread_.joinable())
        writer_thread_.join();
    if (fd_ >= 0) {
        ::fdatasync(fd_);
        ::close(fd_);
        fd_ = -1;
    }
}

bool AofLogger::appendCommand(const std::vector<std::string> &parts) {
    if (!opts_.enabled || fd_ < 0)
        return true;
    std::string line = toRespArray(parts);
    std::string line_copy;
    bool need_incr = rewriting_.load();
    if (need_incr)
        line_copy = line; // 复制一份用于增量缓冲
    int64_t my_seq = 0;
    {
        std::lock_guard<std::mutex> lg(mtx_);
        pending_bytes_ += line.size();
        my_seq = ++seq_gen_;
        queue_.push_back(AofItem{std::move(line), my_seq});
    }
    if (need_incr) {
        std::lock_guard<std::mutex> lk(incr_mtx_);
        incr_cmds_.emplace_back(std::move(line_copy));
    }
    cv_.notify_one();
    if (opts_.mode == AofMode::kAlways) {
        std::unique_lock<std::mutex> lk(mtx_);
        cv_commit_.wait(
            lk, [&] { return last_synced_seq_ >= my_seq || stop_.load(); });
    }
    return true;
}

bool AofLogger::appendRaw(const std::string &raw_resp) {
    if (!opts_.enabled || fd_ < 0)
        return true;
    std::string line_copy;
    bool need_incr = rewriting_.load();
    if (need_incr)
        line_copy = raw_resp; // 复制用于增量缓冲
    int64_t my_seq = 0;
    {
        std::lock_guard<std::mutex> lg(mtx_);
        pending_bytes_ += raw_resp.size();
        my_seq = ++seq_gen_;
        queue_.push_back(AofItem{std::string(raw_resp), my_seq});
    }
    if (need_incr) {
        std::lock_guard<std::mutex> lk(incr_mtx_);
        incr_cmds_.emplace_back(std::move(line_copy));
    }
    cv_.notify_one();
    if (opts_.mode == AofMode::kAlways) {
        std::unique_lock<std::mutex> lk(mtx_);
        cv_commit_.wait(
            lk, [&] { return last_synced_seq_ >= my_seq || stop_.load(); });
    }
    return true;
}

bool AofLogger::load(KeyValueStore &store, std::string &err) {
    if (!opts_.enabled)
        return true;
    int rfd = ::open(path().c_str(), O_RDONLY);
    if (rfd < 0) {
        // not fatal if file does not exist
        return true;
    }
    std::string buf;
    buf.resize(1 << 20);
    std::string data;
    while (true) {
        ssize_t r = ::read(rfd, buf.data(), buf.size());
        if (r < 0) {
            err = "read AOF failed";
            ::close(rfd);
            return false;
        }
        if (r == 0)
            break;
        data.append(buf.data(), static_cast<size_t>(r));
    }
    ::close(rfd);
    // very simple replay: parse by lines; expect only SET/DEL/EXPIRE subset
    // For robustness, reuse RespParser (not included here to avoid dependency
    // circle). Minimal parse:
    size_t pos = 0;
    auto readLine = [&](std::string &out) -> bool {
        size_t e = data.find("\r\n", pos);
        if (e == std::string::npos)
            return false;
        out.assign(data.data() + pos, e - pos);
        pos = e + 2;
        return true;
    };
    while (pos < data.size()) {
        if (data[pos++] != '*')
            break;
        std::string line;
        if (!readLine(line))
            break;
        int n = std::stoi(line);
        std::vector<std::string> parts;
        parts.reserve(n);
        for (int i = 0; i < n; ++i) {
            if (data[pos++] != '$') {
                err = "bad bulk";
                return false;
            }
            if (!readLine(line)) {
                err = "bad bulk len";
                return false;
            }
            int len = std::stoi(line);
            if (pos + static_cast<size_t>(len) + 2 > data.size()) {
                err = "trunc";
                return false;
            }
            parts.emplace_back(data.data() + pos, static_cast<size_t>(len));
            pos += static_cast<size_t>(len) + 2; // skip CRLF
        }
        if (parts.empty())
            continue;
        std::string cmd;
        cmd.reserve(parts[0].size());
        for (char c : parts[0])
            cmd.push_back(static_cast<char>(::toupper(c)));
        if (cmd == "SET" && parts.size() == 3) {
            store.set(parts[1], parts[2]);
        } else if (cmd == "DEL" && parts.size() >= 2) {
            std::vector<std::string> keys(parts.begin() + 1, parts.end());
            store.del(keys);
        } else if (cmd == "EXPIRE" && parts.size() == 3) {
            int64_t sec = std::stoll(parts[2]);
            store.expire(parts[1], sec);
        }
    }
    return true;
}

} // namespace mini_redis

namespace mini_redis {

void AofLogger::writerLoop() {
    const size_t kBatchBytes =
        opts_.batch_bytes > 0 ? opts_.batch_bytes : (64 * 1024);
    const int kMaxIov = 64;
    const auto kWaitNs = std::chrono::microseconds(
        opts_.batch_wait_us > 0 ? opts_.batch_wait_us : 1000);

    std::vector<AofItem> local;
    local.reserve(256);

    while (!stop_.load()) {
        // 若需要暂停以进行原子切换，则进入暂停状态
        if (pause_writer_.load()) {
            std::unique_lock<std::mutex> lk(pause_mtx_);
            writer_is_paused_ = true;
            cv_pause_.notify_all();
            cv_pause_.wait(
                lk, [&] { return !pause_writer_.load() || stop_.load(); });
            writer_is_paused_ = false;
            if (stop_.load())
                break;
        }
        local.clear();
        size_t bytes = 0;
        {
            std::unique_lock<std::mutex> lk(mtx_);
            if (queue_.empty()) {
                cv_.wait_for(lk, kWaitNs,
                             [&] { return stop_.load() || !queue_.empty(); });
            }
            while (!queue_.empty() && (bytes < kBatchBytes) &&
                   (int)local.size() < kMaxIov) {
                local.emplace_back(std::move(queue_.front()));
                bytes += local.back().data.size();
                queue_.pop_front();
            }
            if (pending_bytes_ >= bytes)
                pending_bytes_ -= bytes;
            else
                pending_bytes_ = 0;
        }

        if (local.empty()) {
            // everysec 模式周期性刷盘
            if (opts_.mode == AofMode::kEverySec) {
                auto now = std::chrono::steady_clock::now();
                auto interval = std::chrono::milliseconds(
                    opts_.sync_interval_ms > 0 ? opts_.sync_interval_ms : 1000);
                if (now - last_sync_tp_ >= interval) {
                    if (fd_ >= 0)
                        ::fdatasync(fd_);
                    last_sync_tp_ = now;
                }
            }
            continue;
        }

        // 组装 iovec
        struct iovec iov[kMaxIov];
        int iovcnt = 0;
        for (auto &it : local) {
            if (iovcnt >= kMaxIov)
                break;
            iov[iovcnt].iov_base = const_cast<char *>(it.data.data());
            iov[iovcnt].iov_len = it.data.size();
            ++iovcnt;
        }

        // 聚合写入，处理部分写
        int start_idx = 0;
        size_t start_off = 0;
        while (start_idx < iovcnt) {
            ssize_t w = ::writev(fd_, &iov[start_idx], iovcnt - start_idx);
            if (w < 0) {
                // 出错：简单退让，避免忙等
                ::usleep(1000);
                break;
            }
            size_t rem = static_cast<size_t>(w);
            while (rem > 0 && start_idx < iovcnt) {
                size_t avail = iov[start_idx].iov_len - start_off;
                if (rem < avail) {
                    start_off += rem;
                    iov[start_idx].iov_base =
                        static_cast<char *>(iov[start_idx].iov_base) + rem;
                    iov[start_idx].iov_len = avail - rem;
                    rem = 0;
                } else {
                    rem -= avail;
                    ++start_idx;
                    start_off = 0;
                }
            }
            if (w == 0)
                break; // 不太可能，但防止死循环
        }

        // Linux 可选：触发后台回写，平滑尾部写放大
#ifdef __linux__
        if (opts_.use_sync_file_range && bytes >= opts_.sfr_min_bytes) {
            // 对最近写入的区间进行提示。这里为了简化，使用整个文件范围（可能较重），可进一步优化记录
            // offset
            off_t cur = ::lseek(fd_, 0, SEEK_END);
            if (cur > 0) {
                // 提示内核把 [cur-bytes, cur) 写回磁盘
                off_t start = cur - static_cast<off_t>(bytes);
                if (start < 0)
                    start = 0;
                // SYNC_FILE_RANGE_WRITE: 发起写回请求但不等待完成
                (void)::sync_file_range(fd_, start, static_cast<off_t>(bytes),
                                        SYNC_FILE_RANGE_WRITE);
            }
        }
#endif

        // 模式处理
        if (opts_.mode == AofMode::kAlways) {
            ::fdatasync(fd_);
#ifdef __linux__
            if (opts_.fadvise_dontneed_after_sync) {
                off_t cur2 = ::lseek(fd_, 0, SEEK_END);
                if (cur2 > 0) {
                    (void)::posix_fadvise(fd_, 0, cur2, POSIX_FADV_DONTNEED);
                }
            }
#endif
            // 更新已提交序号并唤醒等待者
            int64_t max_seq = 0;
            for (auto &it : local)
                max_seq = std::max(max_seq, it.seq);
            {
                std::lock_guard<std::mutex> lg(mtx_);
                last_synced_seq_ = std::max(last_synced_seq_, max_seq);
            }
            cv_commit_.notify_all();
        } else if (opts_.mode == AofMode::kEverySec) {
            auto now = std::chrono::steady_clock::now();
            auto interval = std::chrono::milliseconds(
                opts_.sync_interval_ms > 0 ? opts_.sync_interval_ms : 1000);
            if (now - last_sync_tp_ >= interval) {
                ::fdatasync(fd_);
                last_sync_tp_ = now;
#ifdef __linux__
                if (opts_.fadvise_dontneed_after_sync) {
                    off_t cur3 = ::lseek(fd_, 0, SEEK_END);
                    if (cur3 > 0) {
                        (void)::posix_fadvise(fd_, 0, cur3,
                                              POSIX_FADV_DONTNEED);
                    }
                }
#endif
            }
        }
    }

    // 退出前 flush
    if (fd_ >= 0) {
        // 把剩余队列写完
        while (true) {
            std::vector<AofItem> rest;
            size_t bytes = 0;
            {
                std::lock_guard<std::mutex> lg(mtx_);
                while (!queue_.empty() && (int)rest.size() < 64) {
                    rest.emplace_back(std::move(queue_.front()));
                    bytes += rest.back().data.size();
                    queue_.pop_front();
                }
                if (pending_bytes_ >= bytes)
                    pending_bytes_ -= bytes;
                else
                    pending_bytes_ = 0;
            }
            if (rest.empty())
                break;
            struct iovec iov2[64];
            int n = 0;
            for (auto &it : rest) {
                iov2[n].iov_base = const_cast<char *>(it.data.data());
                iov2[n].iov_len = it.data.size();
                ++n;
            }
            int start_idx2 = 0;
            size_t start_off2 = 0;
            while (start_idx2 < n) {
                ssize_t w2 = ::writev(fd_, &iov2[start_idx2], n - start_idx2);
                if (w2 < 0) {
                    if (errno == EINTR || errno == EAGAIN)
                        continue;
                    else
                        break;
                }
                size_t rem2 = static_cast<size_t>(w2);
                while (rem2 > 0 && start_idx2 < n) {
                    size_t avail2 = iov2[start_idx2].iov_len - start_off2;
                    if (rem2 < avail2) {
                        start_off2 += rem2;
                        iov2[start_idx2].iov_base =
                            static_cast<char *>(iov2[start_idx2].iov_base) +
                            rem2;
                        iov2[start_idx2].iov_len = avail2 - rem2;
                        rem2 = 0;
                    } else {
                        rem2 -= avail2;
                        ++start_idx2;
                        start_off2 = 0;
                    }
                }
                if (w2 == 0)
                    break;
            }
        }
        ::fdatasync(fd_);
    }
}

bool AofLogger::bgRewrite(KeyValueStore &store, std::string &err) {
    if (!opts_.enabled) {
        err = "aof disabled";
        return false;
    }
    bool expected = false;
    if (!rewriting_.compare_exchange_strong(expected, true)) {
        err = "rewrite already running";
        return false;
    }
    rewriter_thread_ = std::thread(&AofLogger::rewriterLoop, this, &store);
    return true;
}

static std::string joinPath(const std::string &dir, const std::string &file);

void AofLogger::rewriterLoop(KeyValueStore *store) {
    // 1) 生成临时文件路径
    std::string tmp_path = joinPath(opts_.dir, opts_.filename + ".rewrite.tmp");
    int wfd = ::open(tmp_path.c_str(), O_CREAT | O_TRUNC | O_WRONLY, 0644);
    if (wfd < 0) {
        rewriting_.store(false);
        return;
    }

    // 2) 遍历快照，输出最小命令集
    // String
    {
        auto snap = store->snapshot();
        for (const auto &kv : snap) {
            const std::string &k = kv.first;
            const auto &r = kv.second;
            std::vector<std::string> parts = {"SET", k, r.value};
            std::string line = toRespArray(parts);
            writeAllFD(wfd, line.data(), line.size());
            if (r.expire_at_ms > 0) {
                int64_t now =
                    std::chrono::duration_cast<std::chrono::milliseconds>(
                        std::chrono::steady_clock::now().time_since_epoch())
                        .count();
                int64_t ttl = (r.expire_at_ms - now) / 1000;
                if (ttl < 1)
                    ttl = 1;
                std::vector<std::string> e = {"EXPIRE", k, std::to_string(ttl)};
                std::string el = toRespArray(e);
                writeAllFD(wfd, el.data(), el.size());
            }
        }
    }
    // Hash
    {
        auto snap = store->snapshotHash();
        for (const auto &kv : snap) {
            const std::string &key = kv.first;
            const auto &h = kv.second;
            for (const auto &fv : h.fields) {
                std::vector<std::string> parts = {"HSET", key, fv.first,
                                                  fv.second};
                std::string line = toRespArray(parts);
                writeAllFD(wfd, line.data(), line.size());
            }
            if (h.expire_at_ms > 0) {
                int64_t now =
                    std::chrono::duration_cast<std::chrono::milliseconds>(
                        std::chrono::steady_clock::now().time_since_epoch())
                        .count();
                int64_t ttl = (h.expire_at_ms - now) / 1000;
                if (ttl < 1)
                    ttl = 1;
                std::vector<std::string> e = {"EXPIRE", key,
                                              std::to_string(ttl)};
                std::string el = toRespArray(e);
                writeAllFD(wfd, el.data(), el.size());
            }
        }
    }
    // ZSet
    {
        auto snap = store->snapshotZSet();
        for (const auto &flat : snap) {
            for (const auto &it : flat.items) {
                std::vector<std::string> parts = {
                    "ZADD", flat.key, std::to_string(it.first), it.second};
                std::string line = toRespArray(parts);
                writeAllFD(wfd, line.data(), line.size());
            }
            if (flat.expire_at_ms > 0) {
                int64_t now =
                    std::chrono::duration_cast<std::chrono::milliseconds>(
                        std::chrono::steady_clock::now().time_since_epoch())
                        .count();
                int64_t ttl = (flat.expire_at_ms - now) / 1000;
                if (ttl < 1)
                    ttl = 1;
                std::vector<std::string> e = {"EXPIRE", flat.key,
                                              std::to_string(ttl)};
                std::string el = toRespArray(e);
                writeAllFD(wfd, el.data(), el.size());
            }
        }
    }

    // 3) 进入切换阶段：暂停 writer，写入最终增量并原子替换
    pause_writer_.store(true);
    {
        std::unique_lock<std::mutex> lk(pause_mtx_);
        cv_pause_.wait(lk, [&] { return writer_is_paused_; });
    }
    // 在 writer 暂停期间，阻塞增量缓冲追加，确保没有遗漏
    {
        std::lock_guard<std::mutex> lg(incr_mtx_);
        for (const auto &s : incr_cmds_) {
            writeAllFD(wfd, s.data(), s.size());
        }
        incr_cmds_.clear();
    }
    ::fdatasync(wfd);
    // 原子替换并切换 fd
    {
        std::string final_path = path();
        ::close(fd_);
        ::close(wfd);
        ::rename(tmp_path.c_str(), final_path.c_str());
        fd_ = ::open(final_path.c_str(), O_CREAT | O_APPEND | O_WRONLY, 0644);
        // fsync 目录，保证 rename 持久
        int dfd = ::open(opts_.dir.c_str(), O_RDONLY);
        if (dfd >= 0) {
            ::fsync(dfd);
            ::close(dfd);
        }
    }
    pause_writer_.store(false);
    cv_pause_.notify_all();
    // 清理增量
    {
        std::lock_guard<std::mutex> lg(incr_mtx_);
        incr_cmds_.clear();
    }
    rewriting_.store(false);
}

} // namespace mini_redis
