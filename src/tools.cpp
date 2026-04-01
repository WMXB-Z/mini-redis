#include "mini_redis/tools.hpp"

namespace mini_redis {

// 路径字符串的拼接
std::string joinPath(const std::string &dir, const std::string &file) {
    if (dir.empty())
        return file;
    if (dir.back() == '/')
        return dir + file;
    return dir + "/" + file;
}


// 将数据完整写入文件中
bool writeAllFD(int fd, const char *data, size_t len) {
    size_t off = 0;
    while (off < len) {
        ssize_t w = ::write(fd, data + off, len - off);
        if (w > 0) {
            off += static_cast<size_t>(w);
            continue;
        }
        if (w < 0 && (errno == EINTR || errno == EAGAIN)) {
            continue;
        }
        return false;
    }
    return true;
}

}