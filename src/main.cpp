#include "mini_redis/aof.hpp"
#include "mini_redis/config.hpp"
#include "mini_redis/config_loader.hpp"
#include "mini_redis/server.hpp"
#include <chrono>
#include <csignal>
#include <cstring>
#include <iostream>
#include <string>
#include <thread>

namespace mini_redis {

//全局信号标志，用于标记“服务器是否应该停止”
// volatile：防止编译器优化（因为它可能被信号中断修改）
// sig_atomic_t：保证在信号处理函数中原子修改安全
static volatile std::sig_atomic_t g_should_stop = 0;

// 通知主循环该退出了
// 当收到信号（如 Ctrl+C）时：忽略参数signum，并把 g_should_stop 设为 1
void handle_signal(int signum) {
    (void)signum;
    g_should_stop = 1;
}

// 打印帮助信息
void print_usage(const char *argv0) {
    std::cout << "mini-redis usage:\n"
              << "  " << argv0
              << " [--port <port>] [--bind <ip>] [--config <file>]"
              << std::endl;
}

// 解析命令行参数，并填充ServerConfig
bool parse_args(int argc, char **argv, ServerConfig &out_config) {
    for (int i = 1; i < argc; ++i) {
        const std::string arg = argv[i];
        if (arg == "--port" && i + 1 < argc) {  //解析端口号
            out_config.port = static_cast<uint16_t>(std::stoi(argv[++i]));
        } else if (arg == "--bind" && i + 1 < argc) {  //解析IP号
            out_config.bind_address = argv[++i];
        } else if (arg == "--config" && i + 1 < argc) { //解析配置文件位置，并通过文件加载配置
            std::string file = argv[++i];
            std::string err;
            if (!mini_redis::loadConfigFromFile(file, out_config, err)) {
                std::cerr << err << std::endl;
                return false;
            }
        } else if (arg == "-h" || arg == "--help") {    //打印帮助信息
            print_usage(argv[0]);
            return false;
        } else {//参数格式不对
            std::cerr << "Unknown argument: " << arg << std::endl;
            print_usage(argv[0]);
            return false;
        }
    }
    return true;
}

// 启动服务
int run_server(const ServerConfig &config) {
    // SIGINT：Ctrl + C
    // SIGTERM：kill 命令
    // 收到这些信号时调用handle_signal
    std::signal(SIGINT, handle_signal);
    std::signal(SIGTERM, handle_signal);
    mini_redis::Server srv(config);

    return srv.run();
}
} // namespace mini_redis


int main(int argc, char **argv) {
    mini_redis::ServerConfig config;

    //对输入的命令行进行解析，并得到配置类的对象
    if (!mini_redis::parse_args(argc, argv, config)) {
        return 1;
    }  
    // 根据配置类的对象初始化redis中各配置参数，并启动服务
    return mini_redis::run_server(config);
}
