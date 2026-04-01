#pragma once

#include <string>

#include "mini_redis/config.hpp"

namespace mini_redis {

class KeyValueStore;

// 定义一个类Rdb，表示Redis数据库快照操作对象。
// 它封装了保存（save）、加载（load）RDB 文件，以及获取文件路径的逻辑。
class Rdb {
public:
    // 默认构造函数
    Rdb() = default;
    // 带参数构造函数，explicit:防止隐式类型转换
    explicit Rdb(const RdbOptions &opts) : opts_(opts) {}

    // 提供一个公有方法，可以修改 Rdb 对象的配置。
    // 例如，可以临时开启 RDB 保存功能，或者修改保存路径、文件名等。
    // store：数据库对象（键值存储），里面有所有数据。
    // err：如果保存失败，将错误信息写入该字符串。
    // 返回值： 加载成功则true，加载失败则false
    void setOptions(const RdbOptions &opts) { 
        opts_ = opts; 
    }

    // 保存 RDB 快照
    // store：目标数据库对象，会被加载的 RDB 数据覆盖。
    // err：错误信息输出。
    // 返回值： 加载成功则true，加载失败则false
    bool save(const KeyValueStore &store, std::string &err) const;


    // 加载 RDB 文件
    // store：目标数据库对象，会被加载的 RDB 数据覆盖。
    // err：错误信息输出。
    // 返回值： 加载成功则true，加载失败则false
    bool load(KeyValueStore &store, std::string &err) const;

    // 获取 RDB 文件路径
    std::string path() const;

private:
    // RDB的配置选项，例如是否启用保存、保存路径、文件名等
    RdbOptions opts_{};
};

} // namespace mini_redis
