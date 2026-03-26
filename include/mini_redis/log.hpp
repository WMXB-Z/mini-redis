#pragma once

#include <chrono>
#include <ctime>
#include <iostream>
#include <string>

namespace mini_redis {

// 获取当前系统时间，并格式化为字符串（人类可读）返回，；例如"2026-03-23 15:42:10"
inline std::string nowTime() {
    using namespace std::chrono;
    auto t = system_clock::to_time_t(system_clock::now());
    char buf[64];
    // %F → YYYY-MM-DD
    // %T → HH:MM:SS
    std::strftime(buf, sizeof(buf), "%F %T", std::localtime(&t));
    return std::string(buf);
}

// 使用do{}while(0)是经典宏写法（非常重要），相当于将宏中的语句打包成一个语句块
#define MR_LOG(level, msg)                                                     \
    do {                                                                       \
        std::cerr << "[" << nowTime() << "] [" << level << "] " << msg         \
                  << std::endl;                                                \
    } while (0)

} // namespace mini_redis
