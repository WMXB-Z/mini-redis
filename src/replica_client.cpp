#include "mini_redis/replica_client.hpp"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
#include <cstring>

#include "mini_redis/aof.hpp"
#include "mini_redis/kv.hpp"
#include "mini_redis/rdb.hpp"
#include "mini_redis/resp.hpp"
#include "mini_redis/state.hpp"

using mini_redis::g_store;

namespace mini_redis {

ReplicaClient::ReplicaClient(const ServerConfig& cfg) : cfg_(cfg) {}
ReplicaClient::~ReplicaClient() { stop(); }

void ReplicaClient::start() {
    if (!cfg_.replica.enabled) return;
    running_ = true;
    th_ = std::thread([this] { threadMain(); });
}

void ReplicaClient::stop() {
    if (th_.joinable()) {
        running_ = false;
        th_.join();
    }
}

// 通过启动的后台线程，与主节点redis建立连接，完成主从复制
void ReplicaClient::threadMain() {
    // 创建一个客户端连接套接字（用于和作为服务器的主节点建立TCP连接）
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return;
    sockaddr_in addr{};
    addr.sin_family = AF_INET;//指定要连接的服务器的IPv4
    addr.sin_port = htons(cfg_.replica.master_port);//指定要连接的服务器端口，并转为网络字节序
    ::inet_pton(AF_INET, cfg_.replica.master_host.c_str(), &addr.sin_addr); //把字符串 IP（如 "127.0.0.1"）转成二进制

    if (::connect(fd, (sockaddr*)&addr, sizeof(addr)) < 0) {
        ::close(fd);    
        return;
    }

    // send SYNC/PSYNC
    std::string first;
    // 首次同步方式使用SYNC（全量同步），非首次使用PSYNC（增量同步）
    if (last_offset_ > 0) {
        first = toRespArray({std::string("PSYNC"), std::to_string(last_offset_)});
    } else {
        first = toRespArray({std::string("SYNC")});
    }
    // 向服务器发送指令
    ::send(fd, first.data(), first.size(), 0);

    // read RDB bulk
    RespParser parser;
    std::string buf(8192, '\0');

    // 这里采用的是阻塞I/O，调用recv()时，如果没有数据可读，线程会被挂起直到有数据到来或者连接关闭
    while (running_) {
        ssize_t r = ::recv(fd, buf.data(), buf.size(), 0);
        if (r <= 0) break;
        parser.append(std::string_view(buf.data(), static_cast<size_t>(r)));
        while (true) {
            auto v = parser.tryParseOne();
            if (!v.has_value()) break;  //数据不完整，等下一次解析

            if (v->type == RespType::kBulkString) {  // 情况一：RDB同步（全量同步）
                RdbOptions ropts = cfg_.rdb;
                if (!ropts.enabled) ropts.enabled = true;   //确保开启RDB同步
                
                // 根据配置信息类ropts，创建RDB对象，打开RDB文件并写入
                Rdb r(ropts);
                std::string path = r.path();
                FILE* f = ::fopen(path.c_str(), "wb");
                if (!f) return;
                fwrite(v->bulk.data(), 1, v->bulk.size(), f);
                fclose(f);
                std::string err;
                r.load(g_store, err);//从RDB文件加载数据到g_store，完成全量同步
            } else if (v->type == RespType::kArray) {   // 情况二：命令流（增量同步）
                if (v->array.empty()) continue;
                std::string cmd;
                for (char c : v->array[0].bulk)
                    cmd.push_back(static_cast<char>(::toupper(c)));

                if (cmd == "SET" && v->array.size() == 3) {
                    g_store.set(v->array[1].bulk, v->array[2].bulk);
                } else if (cmd == "DEL" && v->array.size() >= 2) {
                    std::vector<std::string> keys;
                    for (size_t i = 1; i < v->array.size(); ++i)
                        keys.emplace_back(v->array[i].bulk);
                    g_store.del(keys);
                } else if (cmd == "EXPIRE" && v->array.size() == 3) {
                    int64_t s = std::stoll(v->array[2].bulk);
                    g_store.expire(v->array[1].bulk, s);
                } else if (cmd == "HSET" && v->array.size() == 4) {
                    g_store.hset(v->array[1].bulk, v->array[2].bulk,
                                 v->array[3].bulk);
                } else if (cmd == "HDEL" && v->array.size() >= 3) {
                    std::vector<std::string> fs;
                    for (size_t i = 2; i < v->array.size(); ++i)
                        fs.emplace_back(v->array[i].bulk);
                    g_store.hdel(v->array[1].bulk, fs);
                } else if (cmd == "ZADD" && v->array.size() == 4) {
                    double sc = std::stod(v->array[2].bulk);
                    g_store.zadd(v->array[1].bulk, sc, v->array[3].bulk);
                } else if (cmd == "ZREM" && v->array.size() >= 3) {
                    std::vector<std::string> ms;
                    for (size_t i = 2; i < v->array.size(); ++i)
                        ms.emplace_back(v->array[i].bulk);
                    g_store.zrem(v->array[1].bulk, ms);
                } 
            }else if (v->type == RespType::kSimpleString) {// 更新同步的偏移量
                const std::string& s = v->bulk;
                if (s.rfind("OFFSET ", 0) == 0) {   //表示s这一消息的开头就是OFFSET
                    try {
                        last_offset_ = std::stoll(s.substr(8)); //更新同步的偏移量
                    } catch (...) {
                    }
                }
            }
        }
    }
    ::close(fd);
}

}  // namespace mini_redis
