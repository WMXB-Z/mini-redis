#pragma once
#include<string>
#include <unistd.h>
namespace mini_redis {

// 路径字符串的拼接
std::string joinPath(const std::string &dir, const std::string &file);

// 将数据完整写入文件中
bool writeAllFD(int fd, const char *data, size_t len);

}