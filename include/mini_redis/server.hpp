#pragma once
#include <string>
#include "mini_redis/config.hpp"
namespace mini_redis {

class Server {
public:
    explicit Server(const ServerConfig &config);
    ~Server();
    int run();

private:
    int setupListen();
    int setupEpoll();
    int loop();

private:
    const ServerConfig &config_;
    int listen_fd_ = -1;
    int epoll_fd_ = -1;
    int timer_fd_ = -1;
};

} // namespace mini_redis
