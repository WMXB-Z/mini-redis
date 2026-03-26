#pragma once

#include <string>

#include "mini_redis/config.hpp"

namespace mini_redis {

// 从简单的 key=value 配置文件加载配置；支持注释行（# 开头）与空白
// 支持项：port、bind_address
// 返回 true 表示加载成功；失败时 err 会填充原因
bool loadConfigFromFile(const std::string &path, ServerConfig &cfg, std::string &err);

} // namespace mini_redis
