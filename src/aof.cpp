#include "mini_redis/aof.hpp"

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <errno.h>
#include <filesystem>
#include <optional>
#include <sys/stat.h>
#include <sys/uio.h>

#include "mini_redis/kv.hpp"
#include "mini_redis/log.hpp"
#include "mini_redis/tools.hpp"
#include "mini_redis/resp.hpp"

namespace mini_redis {

// // 路径字符串的拼接
// static std::string joinPath(const std::string &dir, const std::string &file) {
//     if (dir.empty())
//         return file;
//     if (dir.back() == '/')
//         return dir + file;
//     return dir + "/" + file;
// }

// // 将数据完整写入文件中
// static bool writeAllFD(int fd, const char *data, size_t len) {
//     size_t off = 0;
//     while (off < len) {
//         ssize_t w = ::write(fd, data + off, len - off);
//         if (w > 0) {
//             off += static_cast<size_t>(w);
//             continue;
//         }
//         if (w < 0 && (errno == EINTR || errno == EAGAIN)) {
//             continue;
//         }
//         return false;
//     }
//     return true;
// }

// // 将vector数组转为RESP协议下的字符串类型，例如：
// // *2\r\n
// // $3\r\nGET\r\n
// // $4\r\nname\r\n
// std::string respArray(const std::vector<std::string> &parts) {
//     std::string out;
//     out.reserve(16 * parts.size());
//     out.append("*").append(std::to_string(parts.size())).append("\r\n");
//     for (const auto &p : parts) {
//         out.append("$").append(std::to_string(p.size())).append("\r\n");
//         out.append(p).append("\r\n");
//     }
//     return out;
// }

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

// AOF初始化+启动后台写线程
bool AofLogger::init(const AofOptions &opts, std::string &err) {
    opts_ = opts;
    if (!opts_.enabled)
        return true;
    std::error_code ec;
    std::filesystem::create_directories(opts_.dir, ec); //创建AOF文件所在目录（支持递归创建）
    if (ec) {   //有值，则说明文件创建出问题
        err = "mkdir failed: " + opts_.dir;
        return false;
    }
    fd_ = ::open(path().c_str(), O_CREAT | O_APPEND | O_WRONLY, 0644);
    if (fd_ < 0) {
        err = "open AOF failed: " + path();
        return false;
    }
    // Linux中预分配文件空间，降低元数据更新成本
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

// 关闭后台线程
void AofLogger::shutdown() {
    running_.store(false);
    stop_.store(true);
    cv_.notify_all();
    if (writer_thread_.joinable())
        writer_thread_.join();  //关闭后台的追写AOF线程

    if (rewriter_thread_.joinable()) 
        rewriter_thread_.join();  //关闭后台的重写AOF线程
    
    if (fd_ >= 0) {
        ::fdatasync(fd_);//将文件缓冲区的数据 刷入磁盘，保证 AOF 数据持久化
        ::close(fd_);
        fd_ = -1;//标记文件已关闭
    }
}

// 向 AOF（Append-Only File）写入一条命令，传入的参数为字符串数组
bool AofLogger::appendCommand(const std::vector<std::string> &parts) {
    if (!opts_.enabled || fd_ < 0)
        return true;

    std::string line = respArray(parts);
    std::string line_copy;
    bool need_incr = rewriting_.load();
    if (need_incr) //检查是否正在进行 AOF文件的压缩重写
        line_copy = line; // 如果是，则复制一份用于增量缓冲，以便后续追加到压缩后的新AOF文件中
    int64_t my_seq = 0;
    {   //保证队列操作的线程安全
        std::lock_guard<std::mutex> lg(mtx_);
        pending_bytes_ += line.size();
        my_seq = ++seq_gen_;
        queue_.push_back(AofItem{std::move(line), my_seq});
    }
    // //即使 BGREWRITEAOF 最终生成新 AOF 文件，也不能让新命令只写入 incr_cmds_：
    // - queue_ 保证命令在当前 AOF 中立即持久化，防止崩溃丢失。
    // - incr_cmds_ 保证 BGREWRITEAOF完成后新文件也完整。如果不写入queue_，BGREWRITEAOF 完成之前发送崩溃，就会丢失数据
    // 因此必须同时写入两个队列。
    if (need_incr) {
        std::lock_guard<std::mutex> lk(incr_mtx_);
        incr_cmds_.emplace_back(std::move(line_copy));
    }

    cv_.notify_one();
    if (opts_.mode == AofMode::kAlways) {
        std::unique_lock<std::mutex> lk(mtx_);
        cv_commit_.wait(lk, [&] {return last_synced_seq_ >= my_seq || stop_.load(); });
    }
    return true;
}

// 向 AOF（Append-Only File）写入一条命令，传入的RESP原始命令
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
        cv_commit_.wait(lk, [&] { return last_synced_seq_ >= my_seq || stop_.load(); });
    }
    return true;
}

// 从AOF文件中读取已有命令，并将它们重放到 KeyValueStore 中，实现“恢复数据库状态”的功能
bool AofLogger::load(KeyValueStore &store, std::string &err) {
    if (!opts_.enabled)
        return true;
    int rfd = ::open(path().c_str(), O_RDONLY);
    if (rfd < 0) 
        return true;
    
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
    // 为了增强健壮性，本可以复用 RespParser（但这里没有这样做，是为了避免依赖循环）
    // 这里只做最简化的解析，按行解析，只预期处理 SET/DEL/EXPIRE 这几类命令的子集。
    size_t pos = 0;
    // 用于判断是否能读出一行完整数据（以/r/n为结尾）
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
            pos += static_cast<size_t>(len) + 2; // skip /r/n
        }
        
        if (parts.empty())
            continue;

        // parts数组中首个元素为指令符，将其转为大写，并判断其操作类型
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

// AOF 后台写线程主循环
void AofLogger::writerLoop() {
    const size_t kBatchBytes = opts_.batch_bytes > 0 ? opts_.batch_bytes : (64 * 1024);//单次最多写多少字节（软上限）
    const int kMaxIov = 64; //writev 最多支持几个个 buffer（iovec）
    // 如果队列为空，最多等待多久
    const auto kWaitNs = std::chrono::microseconds(opts_.batch_wait_us > 0 ? opts_.batch_wait_us : 1000);

    std::vector<AofItem> local;
    local.reserve(256);//本地缓存，用于批量写入

    while (!stop_.load()) { //线程一直跑，直到 stop

        if (pause_writer_.load()) { //暂停机制（用于 AOF rewrite 等）
            std::unique_lock<std::mutex> lk(pause_mtx_);
            writer_is_paused_ = true;
            cv_pause_.notify_all(); //通知外部，我已经暂停了
            cv_pause_.wait(lk, [&] { return !pause_writer_.load() || stop_.load(); });
            writer_is_paused_ = false;
            if (stop_.load())
                break;
        }

        local.clear();
        size_t bytes = 0;   //记录写入磁盘的字节数
        {
            std::unique_lock<std::mutex> lk(mtx_);
            if (queue_.empty()) {
                cv_.wait_for(lk, kWaitNs, [&] { return stop_.load() || !queue_.empty(); });
            }

            while (!queue_.empty() && (bytes < kBatchBytes) && (int)local.size() < kMaxIov) {
                local.emplace_back(std::move(queue_.front()));
                bytes += local.back().data.size();
                queue_.pop_front();
            }

            if (pending_bytes_ >= bytes)
                pending_bytes_ -= bytes;    //表示“还没写入磁盘的数据量”
            else
                pending_bytes_ = 0;
        }

        if (local.empty()) {
            if (opts_.mode == AofMode::kEverySec) { 
                // everysec 模式周期性刷盘，即使当前没有新数据写入（queue 为空），也必须“定时刷盘”，否则数据可能一直停留在内核缓存里。
                auto now = std::chrono::steady_clock::now();
                auto interval = std::chrono::milliseconds(opts_.sync_interval_ms > 0 ? opts_.sync_interval_ms : 1000);
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
        int start_idx = 0;      //iov的块间偏移量
        size_t start_off = 0;   //iov的块内偏移量
        while (start_idx < iovcnt) {
            ssize_t w = ::writev(fd_, &iov[start_idx], iovcnt - start_idx);
            if (w < 0) { // 出错：简单退让，避免忙等
                ::usleep(1000);
                break;
            }

            size_t rem = static_cast<size_t>(w);
            while (rem > 0 && start_idx < iovcnt) {
                size_t avail = iov[start_idx].iov_len - start_off;
                if (rem < avail) {
                    start_off += rem;
                    // 对这个没写完的块进行调整，将还没写的部分重设为iov的内容
                    iov[start_idx].iov_base = static_cast<char *>(iov[start_idx].iov_base) + rem;
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

        // **脏页（dirty page）**存放在 page cache 里
        // 内核负责管理这些脏页的写回到磁盘
        // 用户程序通常不直接干预写回，除非调用 fdatasync / fsync 或 sync_file_range 等系统调用

        // 正常写 AOF 的流程是：writev → 数据进入 page cache（内核缓存） → 某个时刻 fdatasync → 才真正落盘
        // 如果一直 write，但不提前刷,则fdatasync 时 → 一次性刷大量数据 → 卡顿（延迟抖动）
        // 主动触发后台回写，即告诉内核可以把这段数据刷到磁盘了

        #ifdef __linux__
        if (opts_.use_sync_file_range && bytes >= opts_.sfr_min_bytes) {    //启用主动刷盘，且写入数据达到写回阈值
            off_t cur = ::lseek(fd_, 0, SEEK_END);  //获取当前文件末尾
            if (cur > 0) { // 提示内核把 [cur-bytes, cur) 写回磁盘
                off_t start = cur - static_cast<off_t>(bytes);
                if (start < 0)
                    start = 0;
                // SYNC_FILE_RANGE_WRITE: 发起写回请求但不等待完成(异步非阻塞，不保证落盘即数据不安全)
                (void)::sync_file_range(fd_, start, static_cast<off_t>(bytes), SYNC_FILE_RANGE_WRITE);
            }
        }
        #endif

        // 模式处理
        if (opts_.mode == AofMode::kAlways) {
            // 1）落盘操作
            ::fdatasync(fd_);   //调用系统调用 fdatasync，把 page cache 中对应文件的数据页 立即写回磁盘，阻塞调用

            // 2）更新last_synced_seq_
            // 更新已提交序号并唤醒等待者(这里不直接用local的最后一个元素的seq，而是如此做，是为了一定程度上解耦合)
            int64_t max_seq = 0;
            for (auto &it : local)
                max_seq = std::max(max_seq, it.seq);
            {
                std::lock_guard<std::mutex> lg(mtx_);
                last_synced_seq_ = std::max(last_synced_seq_, max_seq);
            }
            cv_commit_.notify_all();

            // 3）释放已经落盘的缓存
            #ifdef __linux__
            if (opts_.fadvise_dontneed_after_sync) {    //如果用户启用了缓存清除优化，则刷盘后，将缓存中的数据释放
                off_t cur2 = ::lseek(fd_, 0, SEEK_END);
                if (cur2 > 0) {
                    // posix_fadvise 用法：（同步阻塞，保证落盘即数据安全）
                    // 第 1 个参数：文件描述符
                    // 第 2、3 个参数：起始偏移和长度（这里从 0 到文件末尾）
                    // 第 4 个参数：建议内核策略，这里是 POSIX_FADV_DONTNEED → 告诉内核这些缓存页可以丢掉
                    // 这样做就是为了减少AOF写盘过程对内存资源的占用，减少对其它功能的影响
                    (void)::posix_fadvise(fd_, 0, cur2, POSIX_FADV_DONTNEED);
                }
            }
            #endif

        } else if (opts_.mode == AofMode::kEverySec) {
            auto now = std::chrono::steady_clock::now();
            auto interval = std::chrono::milliseconds(
            opts_.sync_interval_ms > 0 ? opts_.sync_interval_ms : 1000);
            if (now - last_sync_tp_ >= interval) {
                ::fdatasync(fd_);   // 阻塞写盘，保证数据安全
                last_sync_tp_ = now;

                // 更新 last_synced_seq_ 并通知等待者
                int64_t max_seq = 0;
                for (auto &it : local)
                    max_seq = std::max(max_seq, it.seq);
                {
                    std::lock_guard<std::mutex> lg(mtx_);
                    last_synced_seq_ = std::max(last_synced_seq_, max_seq);
                }
                cv_commit_.notify_all();

                #ifdef __linux__
                if (opts_.fadvise_dontneed_after_sync) {
                    off_t cur3 = ::lseek(fd_, 0, SEEK_END);
                    if (cur3 > 0) { 
                        (void)::posix_fadvise(fd_, 0, cur3, POSIX_FADV_DONTNEED);
                    }
                }
                #endif
            }
        }

    }

    // 退出前也要flush，保证加缓存队列中的数据存入磁盘
    if (fd_ >= 0) {

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
                if (w2 < 0) {   //处理可能的系统调用失败情况
                    // EINTR：系统调用被信号打断 → 重试。
                    // EAGAIN：资源暂时不可用 → 重试。
                    // 其他错误直接跳出循环。
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

        // 执行刷盘
        ::fdatasync(fd_);
    }
}

// 启动 AOF 后台重写（background rewrite）线程
bool AofLogger::bgRewrite(KeyValueStore &store, std::string &err) {
    if (!opts_.enabled) {
        err = "aof disabled";
        return false;
    }
    bool expected = false;
    // compare_exchange_strong：rewriting_ == expected (false)，就把它改成 true，并返回true
    // 防止多个线程同时进入 rewrite
    if (!rewriting_.compare_exchange_strong(expected, true)) {
        err = "rewrite already running";
        return false;
    }

    if(rewriter_thread_.joinable()) 
        rewriter_thread_.join();

    rewriter_thread_ = std::thread(&AofLogger::rewriterLoop, this, &store);
    return true;
}

// 重写AOF文件（实现压缩效果）
void AofLogger::rewriterLoop(KeyValueStore *store) {
    // 1) 生成临时文件路径
    std::string tmp_path = joinPath(opts_.dir, opts_.filename + ".rewrite.tmp");
    int wfd = ::open(tmp_path.c_str(), O_CREAT | O_TRUNC | O_WRONLY, 0644);
    if (wfd < 0) {
        rewriting_.store(false);
        return;
    }

    // 2) 遍历快照，输出最小命令集
    // 把当前内存里string类型的 KV 数据“重写”为一组命令，写入新AOF文件
    {  
        auto snap = store->snapshot();
        for (const auto &kv : snap) {
            const std::string &k = kv.first;
            const auto &r = kv.second;
            std::vector<std::string> parts = {"SET", k, r.value};
            std::string line = respArray(parts);
            writeAllFD(wfd, line.data(), line.size());
            
            //如果有过期时间，则同时生成"EXPIRE key ttl"命令（给key设置过期时间）
             if (r.expire_at_ms > 0) { 
                int64_t now = std::chrono::duration_cast<std::chrono::milliseconds>(
                        std::chrono::steady_clock::now().time_since_epoch())
                        .count();
                int64_t ttl = (r.expire_at_ms - now) / 1000;
                if (ttl < 1)
                    ttl = 1;
                std::vector<std::string> e = {"EXPIRE", k, std::to_string(ttl)};
                std::string el = respArray(e);
                writeAllFD(wfd, el.data(), el.size());
            }
        }
    }
    // Hash类型同上
    {
        auto snap = store->snapshotHash();
        for (const auto &kv : snap) {
            const std::string &key = kv.first;
            const auto &h = kv.second;
            for (const auto &fv : h.fields) {
                std::vector<std::string> parts = {"HSET", key, fv.first,fv.second};
                std::string line = respArray(parts);
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
                std::vector<std::string> e = {"EXPIRE", key, std::to_string(ttl)};
                std::string el = respArray(e);
                writeAllFD(wfd, el.data(), el.size());
            }
        }
    }
    // ZSet类型同上
    {
        auto snap = store->snapshotZSet();
        for (const auto &flat : snap) {
            for (const auto &it : flat.items) {
                std::vector<std::string> parts = {
                    "ZADD", flat.key, std::to_string(it.first), it.second};
                std::string line = respArray(parts);
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
                std::string el = respArray(e);
                writeAllFD(wfd, el.data(), el.size());
            }
        }
    }

    // 3) 进入切换阶段：暂停writer，写入最终增量并原子替换
    pause_writer_.store(true);
    {
        std::unique_lock<std::mutex> lk(pause_mtx_);
        // 修改pause_writer_标志后，重写线程先被阻塞，等到追写线程被阻塞后（即暂停后），重写线程会被唤醒
        cv_pause_.wait(lk, [&] { return writer_is_paused_; });
    }
    // 在追写线程暂停后，要把缓存队列中的数据（还没来得及放入旧AOF中的指令）也写入新AOF中
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

std::string AofLogger::path() const {
    return joinPath(opts_.dir, opts_.filename);
}
} // namespace mini_redis
