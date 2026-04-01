#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace mini_redis {

// 定义RESP 协议支持的数据类型：
// 客户端命令: SET mykey 10086
// RESP Array: *3
//     ├─ BulkString: SET
//     ├─ BulkString: mykey
//     └─ BulkString: 10086

// 服务器响应（如回复状态信息时）: +OK
//     └─ SimpleString: OK

// 服务器响应（如返回计数或长度信息时）:100
//     └─ Integer: 100
// 总结：
// 客户端的命令使用Array → Array的内部元素使用用 Bulk String类型
// 服务器响应 → 根据类型可能用 Simple String / Integer / Bulk String / Error

enum class RespType {
    kSimpleString,
    kError,
    kInteger,
    kBulkString,
    kArray,
    kNull
};

// 定义一个redis中通用的数据容器（类似 JSON）
struct RespValue {
    RespType type = RespType::kNull;    //具体的类型
    std::string bulk; // 用来存放这些基本类型的实际值：simple/bulk/error string or integer text
    std::vector<RespValue> array; // 用来存放array
};

// RESP解析器类：从TCP字节流中解析RESP数据
class RespParser {
public:
    // 把新读取的网络数据追加到内部 buffer
    void append(std::string_view data);

    // 尝试解析一个完整RESP.
    // 情况	    返回
    // 成功解析	RespValue
    // 数据不完整	std::nullopt
    // 解析错误	type = kError
    std::optional<RespValue> tryParseOne();

    // 与上面一致，但返回结果多了原始RESP字符串，返回结果为：{解析结果, 原始RESP字符串}
    // 用于：AOF
    std::optional<std::pair<RespValue, std::string>> tryParseOneWithRaw();

// RESP解析工具函数
private:
    // 解析一行（以\r\n结尾）
    // 输入：pos：当前解析位置（引用，表示会移动）
    // 输出：out_line：解析出的字符串（不含 \r\n）
    bool parseLine(size_t &pos, std::string &out_line);

    // 解析Integer类型
    bool parseInteger(size_t &pos, int64_t &out_value);

    // 解析BulkString类型
    bool parseBulkString(size_t &pos, RespValue &out);

    // 解析SimpleString类型
    bool parseSimple(size_t &pos, RespType t, RespValue &out);

    // 解析Arra类型
    bool parseArray(size_t &pos, RespValue &out);

private:
    // 内部的缓冲区，存储TCP流数据
    std::string buffer_;
};

// RESP序列化工具函数
std::string respSimpleString(std::string_view s);
std::string respError(std::string_view s);
std::string respBulk(std::string_view s);
std::string respNullBulk();
std::string respInteger(int64_t v);
std::string respArray(const std::vector<std::string> &parts);

} // namespace mini_redis
