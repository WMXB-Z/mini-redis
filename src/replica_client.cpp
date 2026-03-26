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

void ReplicaClient::threadMain() {
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return;
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(cfg_.replica.master_port);
    ::inet_pton(AF_INET, cfg_.replica.master_host.c_str(), &addr.sin_addr);
    if (::connect(fd, (sockaddr*)&addr, sizeof(addr)) < 0) {
        ::close(fd);
        return;
    }
    // send SYNC/PSYNC
    std::string first;
    if (last_offset_ > 0) {
        first =
            toRespArray({std::string("PSYNC"), std::to_string(last_offset_)});
    } else {
        first = toRespArray({std::string("SYNC")});
    }
    ::send(fd, first.data(), first.size(), 0);
    // read RDB bulk
    RespParser parser;
    std::string buf(8192, '\0');
    while (running_) {
        ssize_t r = ::recv(fd, buf.data(), buf.size(), 0);
        if (r <= 0) break;
        parser.append(std::string_view(buf.data(), static_cast<size_t>(r)));
        while (true) {
            auto v = parser.tryParseOne();
            if (!v.has_value()) break;
            if (v->type == RespType::kBulkString) {
                // treat as RDB content; parse with Rdb::load by writing temp
                // and reading For simplicity, reuse Rdb by writing to file and
                // calling load
                RdbOptions ropts = cfg_.rdb;
                if (!ropts.enabled) ropts.enabled = true;
                Rdb r(ropts);
                std::string path = r.path();
                FILE* f = ::fopen(path.c_str(), "wb");
                if (!f) return;
                fwrite(v->bulk.data(), 1, v->bulk.size(), f);
                fclose(f);
                std::string err;
                r.load(g_store, err);
            } else if (v->type == RespType::kArray) {
                // command array
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
                } else if (v->type == RespType::kSimpleString) {
                    // parse +OFFSET <num>
                    const std::string& s = v->bulk;
                    if (s.rfind("OFFSET ", 0) == 0) {
                        try {
                            last_offset_ = std::stoll(s.substr(8));
                        } catch (...) {
                        }
                    }
                }
            }
        }
    }
    ::close(fd);
}

}  // namespace mini_redis
