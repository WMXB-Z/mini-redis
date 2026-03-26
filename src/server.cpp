#include "mini_redis/server.hpp"

#include "mini_redis/aof.hpp"
#include "mini_redis/config.hpp"
#include "mini_redis/kv.hpp"
#include "mini_redis/log.hpp"
#include "mini_redis/rdb.hpp"
#include "mini_redis/replica_client.hpp"
#include "mini_redis/resp.hpp"
#include "mini_redis/state.hpp"

#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <sys/timerfd.h>
#include <sys/uio.h>
#include <unistd.h>

#include <cctype>
#include <cstring>
#include <iostream>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

namespace mini_redis {
// ServerConfig moved to config.hpp
namespace { //表示该部分仅在本文件中可见

// 设置非阻塞
int set_nonblocking(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);  //获取当前文件描述符状态标志
    if (flags < 0)  //获取失败
        return -1;
    if (fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0) //在原有 flags 基础上，开启非阻塞
        return -1;
    return 0;
}

//  向epoll实例(epfd)中注册一个文件描述符fd，并指定需要监听的事件类型ev
int add_epoll(int epfd, int fd, uint32_t events) {
    epoll_event ev{};
    ev.events = events; //注册监听事件
    ev.data.fd = fd;    //绑定fd
    return epoll_ctl(epfd, EPOLL_CTL_ADD, fd, &ev); //把一个文件描述符fd注册到epoll实例epfd中，并告诉内核你关心哪些事件
}

// 对epoll实例(epfd)中已注册的文件描述符fd，修改需要监听的事件类型ev
int mod_epoll(int epfd, int fd, uint32_t events) {
    epoll_event ev{};
    ev.events = events;
    ev.data.fd = fd;
    return epoll_ctl(epfd, EPOLL_CTL_MOD, fd, &ev);
}

// 该数据结构用于充当与客户端的通信的缓冲区
struct Conn {
    int fd = -1;
    std::string in = "";                        //输入缓冲区（接收数据）
    std::vector<std::string> out_chunks = {};   // 输出队列（即发送队列，存放待发送的数据块）
    size_t out_iov_idx = 0;                     // 当前发送到第几个块（块间偏移量）
    size_t out_offset = 0;                      // 当前的块发送到哪（块内偏移量）
    RespParser parser = {};                     // 每个连接有一个解析器（支持半包，即数据发送和读取不一定是一条完整数据）
    bool is_replica = false;                    //是否主从复制
};
} // namespace


Server::Server(const ServerConfig &config) : config_(config) {}

Server::~Server() {
    if (listen_fd_ >= 0)
        close(listen_fd_);  //关闭监听socket
    if (epoll_fd_ >= 0)
        close(epoll_fd_);   //关闭epoll
}

// 建立监听socket
int Server::setupListen() {
    listen_fd_ = ::socket(AF_INET, SOCK_STREAM, 0); //创建TCP socket
    if (listen_fd_ < 0) {   //创建失败
        std::perror("socket");
        return -1;
    }

    int yes = 1;
    setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes));    //允许端口复用，以防端口被占用

    // 构造地址
    sockaddr_in addr{};
    addr.sin_family = AF_INET;

    addr.sin_port = htons(static_cast<uint16_t>(config_.port)); //主机序 → 网络序
    if (inet_pton(AF_INET, config_.bind_address.c_str(), &addr.sin_addr) != 1) {    //字符串 IP → 二进制
        MR_LOG("ERROR", "Invalid bind address: " << config_.bind_address);
        return -1;
    }

    // 绑定 IP + 端口
    if (bind(listen_fd_, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) < 0) {
        std::perror("bind");
        return -1;
    }

    // 设置为非阻塞，这样客户执行int conn = accept(listen_fd_, ...);时，即使没有连接，也不会卡住
    if (set_nonblocking(listen_fd_) < 0) {
        std::perror("fcntl");
        return -1;
    }

    // 把 socket 从“普通 socket”变成“监听 socket”
    if (listen(listen_fd_, 512) < 0) {
        std::perror("listen");
        return -1;
    }
    return 0;
}

// 事件系统初始化
int Server::setupEpoll() {
    epoll_fd_ = epoll_create1(0);   //创建epoll实例
    if (epoll_fd_ < 0) {
        std::perror("epoll_create1");
        return -1;
    }

    // 注册监听（监听连接任务）
    if (add_epoll(epoll_fd_, listen_fd_, EPOLLIN | EPOLLET) < 0) {  //注册监听socket
        std::perror("epoll_ctl add");
        return -1;
    }

    // setup periodic timer for active expire scan
    // 创建一个定时器，并获得fd
    //  参数	            含义
    // CLOCK_MONOTONIC	    单调时钟
    // TFD_NONBLOCK	        非阻塞
    // TFD_CLOEXEC	        exec 时关闭
    timer_fd_ = timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK | TFD_CLOEXEC);
    if (timer_fd_ < 0) {
        std::perror("timerfd_create");
        return -1;
    }

    itimerspec its{};   // 声明一个定时器
    its.it_interval.tv_sec = 0;
    its.it_interval.tv_nsec = 200 * 1000 * 1000; // 200ms
    its.it_value = its.it_interval; //初始化时间
    // 启动定时器
    if (timerfd_settime(timer_fd_, 0, &its, nullptr) < 0) {
        std::perror("timerfd_settime");
        return -1;
    }

    // 注册监听(监听IO任务)，每200ms，epoll会触发timer_fd可读
    if (add_epoll(epoll_fd_, timer_fd_, EPOLLIN | EPOLLET) < 0) {
        std::perror("epoll_ctl add timer");
        return -1;
    }
    return 0;
}

KeyValueStore g_store;  //定义内存数据库，即真正存储key-value 数据的地方
static AofLogger g_aof; //记录所有写操作（用于持久化）
static Rdb g_rdb; //做数据库快照（类似 Redis 的 dump.rdb）
static std::vector<std::vector<std::string>> g_repl_queue;  //保存要同步给从节点（replica）的命令（主从复制）

// 判断这个连接是有数据没发送
static inline bool has_pending(const Conn &c) {

    // 还有块没发 或者 最后一个块还没发完
    // 例如：out_chunks = ["abc", "def"]
    // 情况一：out_iov_idx = 0，说明第0个还没发完
    // 情况二：out_iov_idx = 2、out_offset = 1，表示“ef"还没发
    // 注意：out_iov_idx和out_iov_idx均表示下一个待发的位置
    return c.out_iov_idx < c.out_chunks.size() || (c.out_iov_idx == c.out_chunks.size() && c.out_offset != 0);
}

// writev的高性能非阻塞发送机制+零拷贝优化（writev） + 部分写处理 + epoll 驱动续写
// 尽可能立刻把Conn里待发送的数据写到 socket（fd），如果写不完再等EPOLLOUT
static void try_flush_now(int fd, Conn &c, uint32_t &ev) {
    while (has_pending(c)) {
        // 准备一次性发送多个buffer（零拷贝拼接）
        const size_t max_iov = 64;  //一次最多发送buffer大小
        struct iovec iov[max_iov];
        int iovcnt = 0; //buffer的索引

        // 记录当前发送位置
        size_t idx = c.out_iov_idx;
        size_t off = c.out_offset;

        // 最多取 max_iov（预设64）个chunk，组成一次writev
        while (idx < c.out_chunks.size() && iovcnt < (int)max_iov) {
            // 获取要读的chunk的数据和大小
            const std::string &s = c.out_chunks[idx];
            const char *base = s.data();
            size_t len = s.size();

            // 如果要读的偏移位置超出该块的大小，说明要读下一块了
            if (off >= len) {
                ++idx;
                off = 0;
                continue;
            }

            // 块chunk的起始位置+偏移位置=要读的起始位置
            iov[iovcnt].iov_base = (void *)(base + off);
            // 表示这个chunk中要读取那部分的大小
            iov[iovcnt].iov_len = len - off;

            ++iovcnt;
            ++idx;
            off = 0;
        }

        // 表示无buffer存放了数据
        if (iovcnt == 0)
            break;

        // 系统调用，一次发送多个buffer（避免拼接字符串）
        ssize_t w = ::writev(fd, iov, iovcnt);

        // 表示成功写了w个字节（可能没写完）
        if (w > 0) {
            size_t rem = (size_t)w; //表示本次系统调用“实际写入内核发送缓冲区的字节数”
            while (rem > 0 && c.out_iov_idx < c.out_chunks.size()) {
                std::string &s = c.out_chunks[c.out_iov_idx];
                size_t avail = s.size() - c.out_offset; //剩余待发送的字节数

                if (rem < avail) {
                    c.out_offset += rem;
                    rem = 0;
                } else {
                    rem -= avail;
                    c.out_offset = 0;
                    ++c.out_iov_idx;
                }
            }

            // 如果全部发送完，则清空发送队列
            if (c.out_iov_idx >= c.out_chunks.size()) {
                c.out_chunks.clear();
                c.out_iov_idx = 0;
                c.out_offset = 0;
            }
        //表示无法写入：例如socket 发送缓冲区满了，暂时不能写
        } else if (w < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {    
            // need EPOLLOUT to continue later
            break;
        } else {
            std::perror("writev");//上述过程出现异常，则打印信息
            ev |= EPOLLRDHUP;   //并标记：这个连接应该关闭
            break;
        }
    }
}

// 用于将发送的数据s加入连接套接字的待发送队列中 out_chunks
static inline void enqueue_out(Conn &c, std::string s) {
    if (!s.empty())
        c.out_chunks.emplace_back(std::move(s));
}

//保存主节点发送给从节点的复制数据。即所谓的replication backlog buffer
// 可以理解为一个循环缓冲区（ring buffer），存储最近的写操作（写命令序列）。
static std::string g_repl_backlog; 
// 这里表示backlog的最大容量，超出这个容量，则会丢弃旧的部分
static const size_t kReplBacklogCap = 4 * 1024 * 1024; // 4MB
// 表示主节点写入的总字节数
static int64_t g_repl_offset = 0;   
// 因为当前backlog中的内容可能为丢弃了旧部分的结果，所以需要一个变量记录现在backlog在整个数据中的位置
static int64_t g_backlog_start_offset = 0; 

// 将新产生的数据data追加到 replication backlog
static void appendToBacklog(const std::string &data) {
    if (g_repl_backlog.size() + data.size() <= kReplBacklogCap) {   //追加后小于总容量，直接追加
        g_repl_backlog.append(data);
    } else {    //追加后大于总容量，则需要清除旧的部分
        size_t need = data.size();
        if (need >= kReplBacklogCap) {
            g_repl_backlog.assign(data.data() + (need - kReplBacklogCap),
                                  kReplBacklogCap);
        } else {
            size_t drop = (g_repl_backlog.size() + need) - kReplBacklogCap;
            g_repl_backlog.erase(0, drop);
            g_repl_backlog.append(data);
        }
    }
    // 更新在总数据中的偏移位置。
    g_backlog_start_offset = g_repl_offset - static_cast<int64_t>(g_repl_backlog.size());
}

// 作用：负责把解析好的RESP请求转成具体操作
// v：已经解析好的RESP数据结构（命令 + 参数）
// raw：原始命令字符串（用于AOF直接追加）
// 返回一个RESP格式的字符串（即作为回复给客户端的处理结果的提示信息）
static std::string handle_command(const RespValue &v, const std::string *raw) {
    // Redis 协议规定：
    // 解析后的命令必须是 数组 且至少有一个元素（命令名）
    if (v.type != RespType::kArray || v.array.empty())
        return respError("ERR protocol error");

    // 第一个元素必须是字符串（命令符）
    const auto &head = v.array[0];
    if (head.type != RespType::kBulkString && head.type != RespType::kSimpleString)
        return respError("ERR wrong type");

    std::string cmd;
    cmd.reserve(head.bulk.size());  //预设cmd的大小，避免扩容开销
    for (char c : head.bulk)    //将命令字符串中的字符挨个转大写
        cmd.push_back(static_cast<char>(::toupper(c))); //转成大写（实现命令大小写不敏感）

    // 根据解析出的具体命令符，进入不同的分支
    if (cmd == "PING") {    //PING [message]:用于检测redis服务器是否在线
        if (v.array.size() <= 1)    //命令为："PING",则服务器返回“PONG”
            return respSimpleString("PONG");

        if (v.array.size() == 2 && v.array[1].type == RespType::kBulkString)    //命令为："PING <message>"，则服务器返回这个message
            return respBulk(v.array[1].bulk);

        return respError("ERR wrong number of arguments for 'PING'");
    }
    if (cmd == "ECHO") {    //ECHO <message>:用于回显客户端发送的massage,和带message的PING基本一样
        if (v.array.size() == 2 && v.array[1].type == RespType::kBulkString)
            return respBulk(v.array[1].bulk);
        return respError("ERR wrong number of arguments for 'ECHO'");
    }
    if (cmd == "SET") {     //SET <key> <value>：用于存储key-value
        if (v.array.size() < 3) //SET 命令至少需要三个元素
            return respError("ERR wrong number of arguments for 'SET'");

        //确保 key 和value 是 Bulk String 类型（RESP 协议要求）
        if (v.array[1].type != RespType::kBulkString || v.array[2].type != RespType::kBulkString)
            return respError("ERR syntax");

        //这是解析第四个可选参数EX(多少秒后过期删除)或PX（多少毫秒后过期删除）
        // 若未设置，则不会进入while 
        std::optional<int64_t> ttl_ms;
        size_t i = 3;
        while (i < v.array.size()) {
            // 确保第四个参数Bulk String 类型（RESP 协议要求）
            if (v.array[i].type != RespType::kBulkString)
                return respError("ERR syntax");

            // 同样，将第四个参数转为全大写
            std::string opt;
            opt.reserve(v.array[i].bulk.size());
            for (char ch : v.array[i].bulk)
                opt.push_back(static_cast<char>(::toupper(ch)));

            // 如果是EX选项
            if (opt == "EX") {
                // 时间参数没有或者类型不合法
                if (i + 1 >= v.array.size() || v.array[i + 1].type != RespType::kBulkString)
                    return respError("ERR syntax");

                try {
                    int64_t sec = std::stoll(v.array[i + 1].bulk);
                    if (sec < 0)
                        return respError("ERR invalid expire time in SET");
                    ttl_ms = sec * 1000;
                } catch (...) {
                    return respError(
                        "ERR value is not an integer or out of range");
                }
                i += 2; //跳过EX、时间数字这两项，继续while判断
                continue;
            } else if (opt == "PX") {   //如果是PX选项
                if (i + 1 >= v.array.size() ||
                    v.array[i + 1].type != RespType::kBulkString)
                    return respError("ERR syntax");
                try {
                    int64_t ms = std::stoll(v.array[i + 1].bulk);
                    if (ms < 0)
                        return respError("ERR invalid expire time in SET");
                    ttl_ms = ms;
                } catch (...) {
                    return respError(
                        "ERR value is not an integer or out of range");
                }
                i += 2;
                continue;
            } else {    //选项不合法
                // unsupported option for now
                return respError("ERR syntax");
            }
        }

        // 这一步是核心：调用存储层将 key/value 写入。TTL可选，如果没有传 EX/PX → ttl_ms = nullopt
        g_store.set(v.array[1].bulk, v.array[2].bulk, ttl_ms);


        // 如果传入了原始 RESP 文本 raw → 直接写入 AOF；否则把 v.array 转成字符串数组记录 AOF
        // 例子：parts = ["SET","mykey","123"] → 写入 AOF
        if (raw)
            g_aof.appendRaw(*raw);
        else {
            std::vector<std::string> parts;
            parts.reserve(v.array.size());
            for (const auto &e : v.array)
                parts.push_back(e.bulk);
            g_aof.appendCommand(parts);
        }

        //生成同样的数组 → 推送到复制队列，用于主从同步
        // 例子：g_repl_queue.push_back(["SET","mykey","123"])
        {
            std::vector<std::string> parts;
            parts.reserve(v.array.size());
            for (const auto &e : v.array)
                parts.push_back(e.bulk);
            g_repl_queue.push_back(std::move(parts));
        }

        return respSimpleString("OK");
    }
    if (cmd == "GET") {     //GET <key>：按key查询value
        if (v.array.size() != 2)
            return respError("ERR wrong number of arguments for 'GET'");
        if (v.array[1].type != RespType::kBulkString)
            return respError("ERR syntax");
        auto val = g_store.get(v.array[1].bulk);
        if (!val.has_value())
            return respNullBulk();
        return respBulk(*val);
    }
    if (cmd == "KEYS") {    //KEYS *：支持按*通配符，查找所有value
        // 允许 KEYS 或 KEYS <pattern>，未带pattern 时等价 '*'
        std::string pattern = "*";
        if (v.array.size() == 2) {
            if (v.array[1].type == RespType::kBulkString || v.array[1].type == RespType::kSimpleString) {
                pattern = v.array[1].bulk;
            } else {
                return respError("ERR syntax");
            }
        } else if (v.array.size() != 1) {
            return respError("ERR wrong number of arguments for 'KEYS'");
        }
        // 仅支持 '*' 通配（返回所有 keys）。复杂模式可后续扩展。
        auto keys = g_store.listKeys();
        if (pattern != "*") {
            // 简易实现：不支持复杂 glob，直接返回空，或可实现简单前缀/后缀匹配
            // 这里先实现 '*'；其它模式后续扩展
            keys.clear();
        }
        std::string out = "*" + std::to_string(keys.size()) + "\r\n";
        for (const auto &k : keys)
            out += respBulk(k);
        return out;
    }
    if (cmd == "FLUSHALL") {    //FLUSHALL SYNC(默认)/ASYNC：清空 Redis 实例中的所有数据库里的所有数据
        if (v.array.size() != 1)
            return respError("ERR wrong number of arguments for 'FLUSHALL'");
        // 清空所有数据结构
        {
            // 直接使用快照删除，更安全的是在 KV 层提供 clear 接口
            auto s1 = g_store.snapshot();
            std::vector<std::string> keys;
            keys.reserve(s1.size());
            for (const auto &kvp : s1)
                keys.push_back(kvp.first);
            g_store.del(keys);
        }
        {
            auto s2 = g_store.snapshotHash();
            for (const auto &kvp : s2) {
                std::vector<std::string> flds;
                for (const auto &fv : kvp.second.fields)
                    flds.push_back(fv.first);
                g_store.hdel(kvp.first, flds);
            }
        }
        {
            auto s3 = g_store.snapshotZSet();
            for (const auto &flat : s3) {
                std::vector<std::string> mems;
                mems.reserve(flat.items.size());
                for (const auto &it : flat.items)
                    mems.push_back(it.second);
                g_store.zrem(flat.key, mems);
            }
        }
        // AOF 记录
        if (raw)
            g_aof.appendRaw(*raw);
        else
            g_aof.appendCommand({"FLUSHALL"});
        // 复制广播
        g_repl_queue.push_back({"FLUSHALL"});
        return respSimpleString("OK");
    }
    if (cmd == "DEL") {     // DEL [<key>...]：删除输入的key对应的数据
        if (v.array.size() < 2)
            return respError("ERR wrong number of arguments for 'DEL'");
        std::vector<std::string> keys;
        keys.reserve(v.array.size() - 1);
        for (size_t i = 1; i < v.array.size(); ++i) {
            if (v.array[i].type != RespType::kBulkString)
                return respError("ERR syntax");
            keys.emplace_back(v.array[i].bulk);
        }

        // 将DEL命令后面的keys对应的数据全部删除
        int removed = g_store.del(keys);

        if (removed > 0) {
            std::vector<std::string> parts;
            parts.reserve(1 + keys.size());
            parts.emplace_back("DEL");
            // 用于AOP
            for (auto &k : keys)
                parts.emplace_back(k);
            if (raw)
                g_aof.appendRaw(*raw);
            else
                g_aof.appendCommand(parts);
            g_repl_queue.push_back(parts);
        }
        return respInteger(removed);
    }
    if (cmd == "EXISTS") {  //EXISTS <key>：判断key的存在性
        if (v.array.size() != 2)
            return respError("ERR wrong number of arguments for 'EXISTS'");
        if (v.array[1].type != RespType::kBulkString)
            return respError("ERR syntax");
        bool ex = g_store.exists(v.array[1].bulk);
        return respInteger(ex ? 1 : 0);
    }
    if (cmd == "EXPIRE") {  //EXPIRE <key> <seconds>：给 key 设置过期时间
        if (v.array.size() != 3)
            return respError("ERR wrong number of arguments for 'EXPIRE'");
        if (v.array[1].type != RespType::kBulkString || v.array[2].type != RespType::kBulkString)
            return respError("ERR syntax");
        try {
            int64_t seconds = std::stoll(v.array[2].bulk);
            bool ok = g_store.expire(v.array[1].bulk, seconds);
            if (ok) {
                if (raw)
                    g_aof.appendRaw(*raw);
                else
                    g_aof.appendCommand(
                        {"EXPIRE", v.array[1].bulk, std::to_string(seconds)});
            }
            if (ok)
                g_repl_queue.push_back(
                    {"EXPIRE", v.array[1].bulk, std::to_string(seconds)});
            return respInteger(ok ? 1 : 0);
        } catch (...) {
            return respError("ERR value is not an integer or out of range");
        }
    }
    if (cmd == "TTL") {     //TTL <key>：查询某个 key 剩余存活时间
        if (v.array.size() != 2)
            return respError("ERR wrong number of arguments for 'TTL'");
        if (v.array[1].type != RespType::kBulkString)
            return respError("ERR syntax");
        int64_t t = g_store.ttl(v.array[1].bulk);
        return respInteger(t);
    }
    if (cmd == "HSET") {    //HSET <hash_key> <field> <value>：在指定哈希 key 中设置字段和值
        if (v.array.size() != 4)
            return respError("ERR wrong number of arguments for 'HSET'");
        if (v.array[1].type != RespType::kBulkString ||
            v.array[2].type != RespType::kBulkString ||
            v.array[3].type != RespType::kBulkString)
            return respError("ERR syntax");
        int created =
            g_store.hset(v.array[1].bulk, v.array[2].bulk, v.array[3].bulk);
        if (raw)
            g_aof.appendRaw(*raw);
        else
            g_aof.appendCommand(
                {"HSET", v.array[1].bulk, v.array[2].bulk, v.array[3].bulk});
        g_repl_queue.push_back(
            {"HSET", v.array[1].bulk, v.array[2].bulk, v.array[3].bulk});
        return respInteger(created);
    }
    if (cmd == "HGET") {    //HGET <hash_key> <field>：获取哈希（Hash）类型数据中某个字段的值
        if (v.array.size() != 3)
            return respError("ERR wrong number of arguments for 'HGET'");
        if (v.array[1].type != RespType::kBulkString ||
            v.array[2].type != RespType::kBulkString)
            return respError("ERR syntax");
        auto val = g_store.hget(v.array[1].bulk, v.array[2].bulk);
        if (!val.has_value())
            return respNullBulk();
        return respBulk(*val);
    }
    if (cmd == "HDEL") {    //HDEL <hash_key> <field1> [<field2> ...]：删除哈希（Hash）类型数据中的一个或多个字段
        if (v.array.size() < 3)
            return respError("ERR wrong number of arguments for 'HDEL'");
        if (v.array[1].type != RespType::kBulkString)
            return respError("ERR syntax");
        std::vector<std::string> fields;
        for (size_t i = 2; i < v.array.size(); ++i) {
            if (v.array[i].type != RespType::kBulkString)
                return respError("ERR syntax");
            fields.emplace_back(v.array[i].bulk);
        }
        int removed = g_store.hdel(v.array[1].bulk, fields);
        if (removed > 0) {
            std::vector<std::string> parts;
            parts.reserve(2 + fields.size());
            parts.emplace_back("HDEL");
            parts.emplace_back(v.array[1].bulk);
            for (auto &f : fields)
                parts.emplace_back(f);
            if (raw)
                g_aof.appendRaw(*raw);
            else
                g_aof.appendCommand(parts);
            g_repl_queue.push_back(parts);
        }
        return respInteger(removed);
    }
    if (cmd == "HEXISTS") { //HEXISTS <hash_key> <field>：判断哈希（Hash）类型数据中某个字段是否存在 的
        if (v.array.size() != 3)
            return respError("ERR wrong number of arguments for 'HEXISTS'");
        if (v.array[1].type != RespType::kBulkString ||
            v.array[2].type != RespType::kBulkString)
            return respError("ERR syntax");
        bool ex = g_store.hexists(v.array[1].bulk, v.array[2].bulk);
        return respInteger(ex ? 1 : 0);
    }
    if (cmd == "HGETALL") { //HGETALL <hash_key>：获取整个哈希表（Hash）中的所有字段及对应的值
        if (v.array.size() != 2)
            return respError("ERR wrong number of arguments for 'HGETALL'");
        if (v.array[1].type != RespType::kBulkString)
            return respError("ERR syntax");
        auto flat = g_store.hgetallFlat(v.array[1].bulk);
        RespValue arr;
        arr.type = RespType::kArray;
        arr.array.reserve(flat.size());
        std::string out = "*" + std::to_string(flat.size()) + "\r\n";
        for (const auto &s : flat) {
            out += respBulk(s);
        }
        return out;
    }
    if (cmd == "HLEN") {    //HLEN <hash_key>：获取哈希（Hash）类型数据中字段数量,它不返回字段的具体值，只告诉你哈希表里有多少个字段。
        if (v.array.size() != 2)
            return respError("ERR wrong number of arguments for 'HLEN'");
        if (v.array[1].type != RespType::kBulkString)
            return respError("ERR syntax");
        int n = g_store.hlen(v.array[1].bulk);
        return respInteger(n);
    }
    if (cmd == "ZADD") {    //ZADD <key> <score> <member>：向有序集合（Sorted Set）添加成员或更新成员的分数
        if (v.array.size() != 4)
            return respError("ERR wrong number of arguments for 'ZADD'");
        if (v.array[1].type != RespType::kBulkString ||
            v.array[2].type != RespType::kBulkString ||
            v.array[3].type != RespType::kBulkString)
            return respError("ERR syntax");
        try {
            double sc = std::stod(v.array[2].bulk);
            int added = g_store.zadd(v.array[1].bulk, sc, v.array[3].bulk);
            if (raw)
                g_aof.appendRaw(*raw);
            else
                g_aof.appendCommand({"ZADD", v.array[1].bulk, v.array[2].bulk,
                                     v.array[3].bulk});
            g_repl_queue.push_back(
                {"ZADD", v.array[1].bulk, v.array[2].bulk, v.array[3].bulk});
            return respInteger(added);
        } catch (...) {
            return respError("ERR value is not a valid float");
        }
    }
    if (cmd == "ZREM") {    //ZREM <key> <member> [member ...]：从有序集合（Sorted Set）中删除一个或多个成员
        if (v.array.size() < 3)
            return respError("ERR wrong number of arguments for 'ZREM'");
        if (v.array[1].type != RespType::kBulkString)
            return respError("ERR syntax");
        std::vector<std::string> members;
        for (size_t i = 2; i < v.array.size(); ++i) {
            if (v.array[i].type != RespType::kBulkString)
                return respError("ERR syntax");
            members.emplace_back(v.array[i].bulk);
        }
        int removed = g_store.zrem(v.array[1].bulk, members);
        if (removed > 0) {
            std::vector<std::string> parts;
            parts.reserve(2 + members.size());
            parts.emplace_back("ZREM");
            parts.emplace_back(v.array[1].bulk);
            for (auto &m : members)
                parts.emplace_back(m);
            if (raw)
                g_aof.appendRaw(*raw);
            else
                g_aof.appendCommand(parts);
            g_repl_queue.push_back(parts);
        }
        return respInteger(removed);
    }
    if (cmd == "ZRANGE") {  //ZRANGE <key> <start> <stop>:获取有序集合（Sorted Set）中指定范围的成员
        if (v.array.size() != 4)
            return respError("ERR wrong number of arguments for 'ZRANGE'");
        if (v.array[1].type != RespType::kBulkString ||
            v.array[2].type != RespType::kBulkString ||
            v.array[3].type != RespType::kBulkString)
            return respError("ERR syntax");
        try {
            int64_t start = std::stoll(v.array[2].bulk);
            int64_t stop = std::stoll(v.array[3].bulk);
            auto members = g_store.zrange(v.array[1].bulk, start, stop);
            std::string out = "*" + std::to_string(members.size()) + "\r\n";
            for (const auto &m : members)
                out += respBulk(m);
            return out;
        } catch (...) {
            return respError("ERR value is not an integer or out of range");
        }
    }
    if (cmd == "ZSCORE") {  //ZSCORE <key> <member>:查询有序集合（Sorted Set）中某个成员的分数
        if (v.array.size() != 3)
            return respError("ERR wrong number of arguments for 'ZSCORE'");
        if (v.array[1].type != RespType::kBulkString ||
            v.array[2].type != RespType::kBulkString)
            return respError("ERR syntax");
        auto s = g_store.zscore(v.array[1].bulk, v.array[2].bulk);
        if (!s.has_value())
            return respNullBulk();
        return respBulk(std::to_string(*s));
    }
    if (cmd == "BGSAVE" || cmd == "SAVE") { //都是 Redis 中 手动触发 RDB（Redis 数据库快照）持久化 的命令，用于把内存中的数据写入磁盘文件（通常是 dump.rdb），但它们的行为方式不同。
        // BGSAVE是异步持久化，Redis会fork一个子进程，子进程负责将数据写入RDB文件，父进程继续处理客户端请求，不会阻塞
        // SAVE是同步持久化：Redis 会 阻塞当前进程，直接把内存数据写入RDB文件，在写入完成之前，Redis无法处理客户端请求
        if (v.array.size() != 1)
            return respError("ERR wrong number of arguments for 'BGSAVE'");
        std::string err;
        if (!g_rdb.save(g_store, err)) {
            return respError(std::string("ERR rdb save failed: ") + err);
        }
        return respSimpleString("OK");
    }
    if (cmd == "BGREWRITEAOF") {    //AOF（Append Only File）重写命令，用于优化 AOF 文件的大小和写入效率。它的作用是 压缩和重写现有的 AOF 文件，但不改变数据内容。
        // Redis fork 出一个子进程
        // 1、子进程扫描当前内存数据
        // 2、将数据生成最小化的写命令（例如 SET key value 或 HSET 等）写入新的临时 AOF 文件
        // 3、完成后用新文件替换旧的 AOF 文件
        // 4、继续追加新写命令
        // 优点：
        // 1、压缩 AOF 文件，减少磁盘占用
        // 2、提升重启速度，因为新文件更小
        // 3、保持数据一致性
        
        if (v.array.size() != 1)
            return respError(
                "ERR wrong number of arguments for 'BGREWRITEAOF'");
        std::string err;
        if (!g_aof.isEnabled())
            return respError("ERR AOF disabled");
        if (!g_aof.bgRewrite(g_store, err)) {
            return respError(std::string("ERR ") + err);
        }
        return respSimpleString("OK");
    }
    if (cmd == "CONFIG") {  //CONFIG GET <pattern>:配置管理命令，它用于 查看或修改 Redis 的运行配置，可以在不重启 Redis 的情况下调整某些参数，或者查询当前配置状态。
        if (v.array.size() < 2)
            return respError("ERR wrong number of arguments for 'CONFIG'");
        if (v.array[1].type != RespType::kBulkString &&
            v.array[1].type != RespType::kSimpleString)
            return respError("ERR syntax");
        std::string sub;
        for (char c : v.array[1].bulk)
            sub.push_back(static_cast<char>(::toupper(c)));
        if (sub == "GET") {
            // 允许 CONFIG GET 与 CONFIG GET <pattern>（未提供时默认 "*")
            std::string pattern = "*";
            if (v.array.size() >= 3) {
                if (v.array[2].type != RespType::kBulkString &&
                    v.array[2].type != RespType::kSimpleString)
                    return respError(
                        "ERR wrong number of arguments for 'CONFIG GET'");
                pattern = v.array[2].bulk;
            } else if (v.array.size() != 2) {
                return respError(
                    "ERR wrong number of arguments for 'CONFIG GET'");
            }
            auto match = [&](const std::string &k) -> bool {
                if (pattern == "*")
                    return true;
                return pattern == k;
            };
            std::vector<std::pair<std::string, std::string>> kvs;
            // minimal set to satisfy tooling
            kvs.emplace_back("appendonly", g_aof.isEnabled() ? "yes" : "no");
            std::string appendfsync;
            switch (g_aof.mode()) {
            case AofMode::kNo:
                appendfsync = "no";
                break;
            case AofMode::kEverySec:
                appendfsync = "everysec";
                break;
            case AofMode::kAlways:
                appendfsync = "always";
                break;
            }
            kvs.emplace_back("appendfsync", appendfsync);
            kvs.emplace_back("dir", "./data");
            kvs.emplace_back("dbfilename", "dump.rdb");
            kvs.emplace_back("save", "");
            kvs.emplace_back("timeout", "0");
            kvs.emplace_back("databases", "16");
            kvs.emplace_back("maxmemory", "0");
            std::string body;
            size_t elems = 0;
            if (pattern == "*") {
                for (auto &p : kvs) {
                    body += respBulk(p.first);
                    body += respBulk(p.second);
                    elems += 2;
                }
            } else {
                for (auto &p : kvs) {
                    if (match(p.first)) {
                        body += respBulk(p.first);
                        body += respBulk(p.second);
                        elems += 2;
                    }
                }
            }
            return "*" + std::to_string(elems) + "\r\n" + body;
        } else if (sub == "RESETSTAT") {
            if (v.array.size() != 2)
                return respError(
                    "ERR wrong number of arguments for 'CONFIG RESETSTAT'");
            return respSimpleString("OK");
        } else {
            return respError("ERR unsupported CONFIG subcommand");
        }
    }
    if (cmd == "INFO") {    //INFO [server/clients/memory]:用于 查看 Redis 服务器的运行状态、统计信息和配置信息。
        // INFO [section] -> ignore section for now
        std::string info;
        info.reserve(512);
        info += "# Server\r\nredis_version:0.1.0\r\nrole:master\r\n";
        info += "# Clients\r\nconnected_clients:0\r\n";
        info += "# "
                "Stats\r\ntotal_connections_received:0\r\ntotal_commands_"
                "processed:0\r\ninstantaneous_ops_per_sec:0\r\n";
        info += "# Persistence\r\naof_enabled:";
        info += (g_aof.isEnabled() ? "1" : "0");
        info += "\r\naof_rewrite_in_progress:0\r\nrdb_bgsave_in_progress:0\r\n";
        info += "# Replication\r\nconnected_slaves:0\r\nmaster_repl_offset:" +
                std::to_string(g_repl_offset) + "\r\n";
        return respBulk(info);
    }
    return respError("ERR unknown command");
}


// 一个基于epoll的高性能事件循环（类似Redis/Reactor模型），而且还带了简单主从复制（replication）逻辑。
int Server::loop() {
    std::unordered_map<int, Conn> conns;    //每个客户端连接的<连接套接字,用户区缓冲区>
    std::vector<epoll_event> events(128);   //epoll 事件缓冲区（最多一次返回 128 个事件）

    // 服务器进行的主循环
    while (true) {
        // vector.data()：返回指向这个数组的指针
        int n = epoll_wait(epoll_fd_, events.data(), static_cast<int>(events.size()), -1);

        if (n < 0) {    //表示监听失败
            if (errno == EINTR) //表示失败原因是被信号中断，立即重试
                continue;
            std::perror("epoll_wait");
            return -1;
        }

        for (int i = 0; i < n; ++i) {   // 处理各个响应的事件
            int fd = events[i].data.fd; //取出epoll事件数组中的中的被监听对象（文件描述符）
            uint32_t ev = events[i].events;//取出要监听事件类型

            // 如果当前被监听的对象与连接有关（表示这次发生的是一个连接事件）
            if (fd == listen_fd_) {
                // 因为用了EPOLLET（边缘触发），必须把连接全部accept完
                while (true) {
                    sockaddr_in cli{};
                    socklen_t len = sizeof(cli);

                    // 接受客户端的连接
                    int cfd = accept(listen_fd_, reinterpret_cast<sockaddr *>(&cli), &len);

                    if (cfd < 0) {  //接受连接失败
                        //EAGAIN:没有更多数据，EWOULDBLOCK：有行为要被阻塞，但设置上是非阻塞，所以返回这个错误
                        if (errno == EAGAIN || errno == EWOULDBLOCK)    
                            break;
                        std::perror("accept");
                        break;
                    }
                    set_nonblocking(cfd);   //设置非阻塞
                    int one = 1;
                    setsockopt(cfd, IPPROTO_TCP, TCP_NODELAY, &one,
                               sizeof(one));    //关闭 Nagle（低延迟）
                    //把新的连接后得到的cfd注册到epoll中，监听相应的一些列事件，ET触发模式（边缘触发）
                    add_epoll(epoll_fd_, cfd, EPOLLIN | EPOLLET | EPOLLRDHUP | EPOLLHUP);   
                    conns.emplace(cfd, Conn{cfd, std::string(),
                                            std::vector<std::string>{}, 0, 0,
                                            RespParser{}, false});  //在unordered_map中就地构造一个元素
                }
                continue;   // 不需要往下了，因为连接事件仅仅是为了建立连接
            }

            //如果当前被监听的对象与定时任务有关（定时任务是周期性要做的事情）
            if (fd == timer_fd_) {
                while (true) {
                    uint64_t ticks;
                    ssize_t _r = ::read(timer_fd_, &ticks, sizeof(ticks));
                    if (_r < 0) {
                        if (errno == EAGAIN || errno == EWOULDBLOCK)
                            break;
                        else
                            break;
                    }
                    if (_r == 0)
                        break;
                }
                g_store.expireScanStep(64); //删除一部分（64）过期的数据（Redis 风格：渐进式删除）
                continue;   // 不需要往下了，因为这就是一个I/O事件
            }

            // 上述两个if都不满足，则说明，当前被监听的对象与io事件有关
            auto it = conns.find(fd);   //找该连接中用于通信的"连接套接字"
            if (it == conns.end())  //表示没有找到
                continue;
            Conn &c = it->second;   //拿到这个"连接套接字"

            // 情况一：EPOLLHUP 或 EPOLLERR → 立即关闭连接
            // 情况二：EPOLLRDHUP → 对方半关闭，先把待发送的数据发送完再关闭
            // 目的是保证服务器不会丢掉还没发出去的回复

            // ev 是 epoll_wait 返回的事件
            // EPOLLHUP → 对端完全关闭连接（挂起/挂断）
            // EPOLLERR → 出现 socket 错误（如连接重置等）
            // 逻辑：如果 完全断开或出现错误，就不能继续通信，需要立刻处理
            if ((ev & EPOLLHUP) || (ev & EPOLLERR)) {
                epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, fd, nullptr);
                close(fd);
                conns.erase(it);
                continue;
            }

            // EPOLLIN->表示socket可读（有数据到达或者对端关闭）
            if (ev & EPOLLIN) {
                char buf[4096];
                while (true) {// 把读的数据放入用户区缓冲区的解析器中
                    ssize_t r = ::read(fd, buf, sizeof(buf));
                    if (r > 0) {
                        c.parser.append( std::string_view(buf, static_cast<size_t>(r)));
                    } else if (r == 0) {    //表示客户端半关闭（EPOLLRDHUP）
                        ev |= EPOLLRDHUP;   //标记当前 fd 已经发生了半关闭事件(对面的连接关了)
                        break;
                    } else {    
                        if (errno == EAGAIN || errno == EWOULDBLOCK)    //EAGAIN/EWOULDBLOCK → 表示数据读完，退出循环
                            break;
                        std::perror("read");
                        ev |= EPOLLRDHUP;   //标记当前fd已经发生了半关闭事件
                        break;
                    }
                }
                
                while (true) {// 循环对RESP消息进行解析并执行
                    // 对解析器中的完整内容进行解析
                    auto maybe = c.parser.tryParseOneWithRaw();

                    if (!maybe.has_value())
                        break;
                    
                    const RespValue &v = maybe->first;  //解析后的结果
                    const std::string &raw = maybe->second; //解析前的RESP协议下的原始字符串

                    if (v.type == RespType::kError) {   //解析后是kError类型
                        enqueue_out(c, respError("ERR protocol error"));    //需要将提示信息发回给客户端
                    } else {
                        // 对redis中的主从复制命令SYNC和PSYNC进行解析
                        if (v.type == RespType::kArray && !v.array.empty() &&
                            (v.array[0].type == RespType::kBulkString ||
                             v.array[0].type == RespType::kSimpleString)) {
                            std::string cmd;
                            cmd.reserve(v.array[0].bulk.size());
                            for (char ch : v.array[0].bulk)
                                cmd.push_back(static_cast<char>(::toupper(ch)));


                            // Redis 复制数据(数据同步)主要有两种方式：
                            // 1）增量复制（PSYNC）
                            // 主节点维护一个 backlog（写命令日志）。
                            // 从节点带上自己的 偏移量 offset 请求增量数据。
                            // 主节点只发送从节点缺失的写命令，从节点执行这些命令，实现同步。
                            // 这种方式才是“把执行过的命令发送给从节点”的场景。

                            // 2）全量复制（SYNC）
                            // 主节点生成一个 RDB 快照（数据库当前状态的二进制文件）。
                            // 从节点接收 RDB 文件并加载，完成第一次同步。
                            // 这种方式是 直接发送数据库状态，不传执行命令。

                            // 第一次同步：通常用 SYNC（RDB 快照）保证从节点有完整数据。
                            // 后续同步：用 PSYNC（命令日志）保证从节点跟上主节点的增量变化。
                            // 这样，即使从节点中途断开，也能通过偏移量恢复增量同步。

                            // PSYNC <offset>：PSYNC命令处理（增量复制）
                            if (cmd == "PSYNC") {
                                // 判断指令是否合法
                                if (v.array.size() == 2 && v.array[1].type == RespType::kBulkString) {
                                    int64_t want = 0;
                                    try {
                                        want = std::stoll(v.array[1].bulk); //std::stoll:将std::string转换为整数
                                    } catch (...) {
                                        want = -1;
                                    }
                                    // 检查是否可以使用 backlog 增量同步
                                    // g_backlog_start_offset 和 g_repl_offset 是服务端维护的 复制偏移量。
                                    // 如果客户端offset在backlog范围内，就直接从 backlog 发送增量数据。
                                    if (want >= g_backlog_start_offset && want <= g_repl_offset) {
                                        size_t start = static_cast<size_t>( want - g_backlog_start_offset);
                                        // 要求：待同步的数据量<backlog中的数据量
                                        if (start < g_repl_backlog.size()) {
                                            c.is_replica = true;    //标记通信的对方为从节点redis
                                           //把当前主节点redis上已经处理的写命令的累计字节数写成RESP格式的字符串，发给从节点redis
                                            std::string off =
                                                "+OFFSET " +
                                                std::to_string(g_repl_offset) +
                                                "\r\n"; 
                                            enqueue_out(c, off);    
                                            enqueue_out(c, g_repl_backlog.substr(start));//把主节点redis上增量的数据，发给从节点redis
                                            continue;
                                        }
                                    }
                                }
                                // fallback to full resync using SYNC path below
                            }
                            // SYNC命令处理（全量复制）
                            if (cmd == "SYNC") {
                                // produce RDB snapshot bytes
                                std::string err;
                                // Save to temp path and read back
                                RdbOptions tmp = config_.rdb;

                                if (!tmp.enabled) {
                                    // 如果 RDB 没启用，就临时开启。
                                    tmp.enabled = true;
                                }

                                Rdb r(tmp); //创建一个RDB对象r，用于生成数据库快照。
                                if (!r.save(g_store, err)) {
                                    enqueue_out( c, respError("ERR sync save failed"));
                                } else {
                                    // 读取RDB文件
                                    std::string path = r.path();
                                    FILE *f = ::fopen(path.c_str(), "rb");
                                    if (!f) {
                                        enqueue_out(c, respError("ERR open rdb"));
                                    } else {
                                        std::string content;
                                        content.resize(0);
                                        char rb[8192];
                                        size_t m;
                                        // 逐块读取文件内容（每次 8192 字节），拼接到content
                                        while ((m = fread(rb, 1, sizeof(rb), f)) > 0)
                                            content.append(rb, m);
                                        fclose(f);  //关闭文件

                                        enqueue_out(c, respBulk(content)); //将文件内容发送给从节点redis
                                        c.is_replica = true;
                                        // 发送当前offset（简单实现：用RESP简单字符串）
                                        std::string off =
                                            "+OFFSET " +
                                            std::to_string(g_repl_offset) +
                                            "\r\n";
                                        enqueue_out(c, std::move(off));
                                    }
                                }
                                continue; // do not pass to normal handler
                            }
                        }

                        enqueue_out(c, handle_command(v, &raw));    //将处理后的结果提示信息，发送给从reids
                        // 尝试把当前连接 c 中待发送的数据，立即写入 socket（fd）发送给客户端。
                        try_flush_now(fd, c, ev);
                    }
                }

                // Redis 主从复制（增量传播）核心逻辑——把最新的写命令广播给所有从节点（replica）
                if (!g_repl_queue.empty()) {
                    for (auto &kv : conns) {   //遍历所有已建立连接
                        Conn &rc = kv.second;
                        if (!rc.is_replica)     //没有启用主从复制的，就无需后续处理
                            continue;
                            
                        for (const auto &parts : g_repl_queue) {//依次广播每条指令
                            std::string cmd = toRespArray(parts);   
                            int64_t next_off = g_repl_offset + static_cast<int64_t>(cmd.size());
                            std::string off = "+OFFSET " + std::to_string(next_off) + "\r\n";
                            appendToBacklog(off);
                            appendToBacklog(cmd);
                            g_repl_offset = next_off;
                            enqueue_out(rc, std::move(off));
                            enqueue_out(rc, std::move(cmd));
                        }
                        if (has_pending(rc)) {      //如果该连接有数据要发，就监听EPOLLOUT
                            mod_epoll(epoll_fd_, rc.fd, EPOLLIN | EPOLLET | EPOLLOUT | EPOLLRDHUP | EPOLLHUP);
                        }
                    }
                    g_repl_queue.clear();
                }
                
                // 如果有数据还没有发送
                if (has_pending(c)) {
                    mod_epoll(epoll_fd_, fd, EPOLLIN | EPOLLET | EPOLLOUT | EPOLLRDHUP | EPOLLHUP);
                }
                
                // 如果半关闭且没有数据待发送。
                if ((ev & EPOLLRDHUP) && !has_pending(c)) {
                    epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, fd, nullptr);
                    close(fd);  //关闭这个被监听的对象（即该连接的连接套接字）
                    conns.erase(it);    //清除这个连接套接字对应的map元素
                    continue;
                }
            }

            // EPOLLOUT表示socket可写入
            if (ev & EPOLLOUT) { 
                // 若缓冲区中还有数据待发送（写到连接套接字中）       
                while (has_pending(c)) {
                    const size_t max_iov = 64;  //限制writev批量写的最大数量
                    struct iovec iov[max_iov];
                    int iovcnt = 0; //要写入的iovec的数量
                    size_t idx = c.out_iov_idx;
                    size_t off = c.out_offset;

                    // 遍历缓冲区中发送队列里待发送的字符串，将它的指针赋值给iovec.iov_base(以便实现批量发送)
                    while (idx < c.out_chunks.size() && iovcnt < (int)max_iov) {
                        const std::string &s = c.out_chunks[idx];
                        const char *base = s.data();
                        size_t len = s.size();
                        if (off >= len) {
                            ++idx;
                            off = 0;
                            continue;
                        }
                        iov[iovcnt].iov_base = (void *)(base + off);
                        iov[iovcnt].iov_len = len - off;
                        ++iovcnt;
                        ++idx;
                        off = 0;
                    }

                    if (iovcnt == 0)
                        break;

                    ssize_t w = ::writev(fd, iov, iovcnt); //writev返回实际写入的字节数
                    if (w > 0) {   
                        size_t rem = (size_t)w;

                        // 这个while的作用，就是用于更新待发送数据的块内偏移量和块间偏移量
                        while (rem > 0 && c.out_iov_idx < c.out_chunks.size()) {
                            std::string &s = c.out_chunks[c.out_iov_idx];

                            size_t avail = s.size() - c.out_offset;//计算当前块剩余未发送字节avail

                            if (rem < avail) {  //如果写入字节少于当前块剩余字节,更新待发送块的移动偏移量
                                c.out_offset += rem;    
                                rem = 0;
                            } else {    //如果写入字节超过当前块剩余字节：移动到下一块，继续减去已写的字节数
                                rem -= avail;
                                c.out_offset = 0;
                                ++c.out_iov_idx;
                            }
                        }

                        if (c.out_iov_idx >= c.out_chunks.size()) { //若发送队列中的数据块已全部发送完
                            c.out_chunks.clear();
                            c.out_iov_idx = 0;
                            c.out_offset = 0;
                        }

                    } else if (w < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {//EAGAIN/EWOULDBLOCK表示内核缓冲区满，需要等待下一次EPOLLOUT。
                        break;
                    } else { //其它错误，则标记 EPOLLRDHUP，表示需要关闭连接，并退出发送的循环过程
                        std::perror("writev");
                        ev |= EPOLLRDHUP;
                        break;
                    }
                }

                //若缓冲区中没有数据要发送，则修改epoll事件，只关注读事件和关闭事件（不再监听EPOLLOUT，避免浪费CPU。）
                if (!has_pending(c)) {
                    mod_epoll(epoll_fd_, fd, EPOLLIN | EPOLLRDHUP | EPOLLHUP);
                    if (ev & EPOLLRDHUP) {
                        epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, fd, nullptr);
                        close(fd);
                        conns.erase(it);
                        continue;
                    }
                }
            }
        }
    }
}

// redis服务器启动的入口函数
int Server::run() {
    if (setupListen() < 0)  //建立一个监听socket
        return -1;
    if (setupEpoll() < 0)   //初始化一个epoll实例
        return -1;

    // 如果配置中启用了 RDB（快照）持久化：
        // 调用 g_rdb.setOptions() 初始化RDB参数
        // 调用 g_rdb.load() 从 RDB 文件加载数据到内存g_store
        // 如果加载失败，记录日志并返回错误
    // g_store 是服务器的 内存数据库实例
    if (config_.rdb.enabled) {
        g_rdb.setOptions(config_.rdb);
        std::string err;
        if (!g_rdb.load(g_store, err)) {
            MR_LOG("ERROR", "RDB load failed: " << err);
            return -1;
        }
    }

    // 如果配置中启用了 AOF（追加文件）持久化：
        // 初始化 AOF（日志文件）
        // 加载 AOF 日志中的操作，恢复到内存数据库 g_store
        // 出现错误时记录日志并返回
    // RDB 是快照，AOF 是操作日志，这两者都能恢复数据，通常先 RDB 再 AOF 以保证完整性
    if (config_.aof.enabled) {
        std::string err;
        if (!g_aof.init(config_.aof, err)) {
            MR_LOG("ERROR", "AOF init failed: " << err);
            return -1;
        }
        if (!g_aof.load(g_store, err)) {
            MR_LOG("ERROR", "AOF load failed: " << err);
            return -1;
        }
    }

    // 打印服务器绑定的IP和端口号
    MR_LOG("INFO", "listening on " << config_.bind_address << ":" << config_.port);
    
    // 如果配置了主从复制，这里创建并启动 副本客户端（ReplicaClient），负责向主节点同步数据
    ReplicaClient repl(config_);
    repl.start();

    // 启动epoll事件循环
    int rc = loop();
    // 服务器退出时停止副本客户端，断开与主节点的同步连接
    repl.stop();
    // 0表示服务器是正常退出，>0表示异常退出
    return rc;
}

} // namespace mini_redis
