#pragma once

#include <string>
#include <thread>

#include "mini_redis/config.hpp"

namespace mini_redis {

class ReplicaClient {
public:
    explicit ReplicaClient(const ServerConfig &cfg);
    ~ReplicaClient();
    void start();
    void stop();

private:
    void threadMain();

private:
    const ServerConfig &cfg_;
    std::thread th_;
    bool running_ = false;
    int64_t last_offset_ = 0;
};

} // namespace mini_redis
