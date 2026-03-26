#include "mini_redis/resp.hpp"
#include <charconv>

// google的代码风格，namespace内部不缩进
namespace mini_redis {

// 以下为RespParser类中各个函数的具体定义

void RespParser::append(std::string_view data) {
    // data 是一段新读到的 TCP 数据,追加网络数据至缓冲区buffer_中
    buffer_.append(data.data(), data.size());
}

// 解析一行，这是其他解析工具的基础
// 输入：pos：当前解析位置（引用，能修改）
// 输出：out_line、结果true
bool RespParser::parseLine(size_t &pos, std::string &out_line) {
    size_t end = buffer_.find("\r\n", pos); // 从pos开始找行结束符 \r\n
    if (end == std::string::npos)           // 表示没有找到
        return false;
    out_line.assign(buffer_.data() + pos, end - pos); // 拷贝这一行（不含 \r\n）
    pos = end + 2; // 将pos后置两个字节，即跳过\r\n
    return true;
}

// 按Interger类型进行解析
bool RespParser::parseInteger(size_t &pos, int64_t &out_value) {
    std::string line;
    if (!parseLine(pos, line))
        return false;
    auto first = line.data(); // 获取char*的起始位置和结束位置
    auto last = line.data() + line.size();
    int64_t v = 0;
    // 把[first, last) 这一段字符解析成整数，存入
    // v。获得解析停止为止ptr、错误码ec
    auto [ptr, ec] = std::from_chars(first, last, v);
    // std::errc()：表示没有错误
    // 解析出错（非法字符or溢出）、没有完全解析完（非纯数字字符串），则解析失败
    if (ec != std::errc() || ptr != last)
        return false;
    out_value = v;
    return true;
}

// 按SimpleString 或 Error类型进行解析
bool RespParser::parseSimple(size_t &pos, RespType t, RespValue &out) {
    std::string s;
    if (!parseLine(pos, s))
        return false;
    out.type = t;
    out.bulk = std::move(s); // 将解析出的一行字符串，直接移动至bulk
    return true;
}

// 按BulkString类型进行解析
bool RespParser::parseBulkString(size_t &pos, RespValue &out) {
    int64_t len = 0;
    if (!parseInteger(pos, len))    //解析首个数字（将作为BulkString的大小）
        return false;

    if (len == -1) {    //特例：$-1\r\n → null
        out.type = RespType::kNull;
        return true;
    }

    if (len < 0)
        return false;

    if (buffer_.size() < pos + static_cast<size_t>(len) + 2)    //必须保证buffer中是完整的bulkString内容
        return false;

    out.type = RespType::kBulkString;

    out.bulk.assign(buffer_.data() + pos, static_cast<size_t>(len));

    pos += static_cast<size_t>(len);

    // 保证协议格式合规
    if (!(pos + 1 < buffer_.size() && buffer_[pos] == '\r' && buffer_[pos + 1] == '\n'))
        return false;
    pos += 2;
    return true;
}

// 按Array格式进行解析：递归解析
bool RespParser::parseArray(size_t &pos, RespValue &out) {
    int64_t count = 0;
    if (!parseInteger(pos, count))
        return false;

    if (count == -1) {
        out.type = RespType::kNull;
        return true;
    }

    if (count < 0)
        return false;

    out.type = RespType::kArray;
    out.array.clear();
    // reserve请求矢量容量至少足以容纳n个元素。如果n大于当前向量容量 ，该函数会使容器重新分配存储空间，使其容量增加到 n（或更大）。
    out.array.reserve(static_cast<size_t>(count));

    // 循环解析每个元素
    for (int64_t i = 0; i < count; ++i) {
        if (pos >= buffer_.size())
            return false;

        char prefix = buffer_[pos++];
        RespValue elem;
        bool ok = false;

        // 根据+ - : $ *这五类标志，按不同的类型进行解析
        switch (prefix) {
        case '+':
            ok = parseSimple(pos, RespType::kSimpleString, elem);
            break;
        case '-':
            ok = parseSimple(pos, RespType::kError, elem);
            break;
        case ':': {
            int64_t v = 0;
            ok = parseInteger(pos, v);
            if (ok) {
                elem.type = RespType::kInteger;
                elem.bulk = std::to_string(v);
            }
            break;
        }
        case '$':
            ok = parseBulkString(pos, elem);
            break;
        case '*':
            ok = parseArray(pos, elem);
            break;
        default:
            return false;
        }

        if (!ok)
            return false;

        out.array.emplace_back(std::move(elem));
    }
    return true;
}

// 尝试解析一个完整RESP
std::optional<RespValue> RespParser::tryParseOne() {
    if (buffer_.empty())
        return std::nullopt;

    // 从头开始解析
    size_t pos = 0;
    char prefix = buffer_[pos++];
    RespValue out;
    bool ok = false;

    // 根据不同的符号标志进入不同的分支进行解析
    switch (prefix) {
    case '+':
        ok = parseSimple(pos, RespType::kSimpleString, out);
        break;
    case '-':
        ok = parseSimple(pos, RespType::kError, out);
        break;
    case ':': {
        int64_t v = 0;
        ok = parseInteger(pos, v);
        if (ok) {
            out.type = RespType::kInteger;
            out.bulk = std::to_string(v);
        }
        break;
    }
    case '$':
        ok = parseBulkString(pos, out);
        break;
    case '*':
        ok = parseArray(pos, out);
        break;
    default:
        return RespValue{RespType::kError, std::string("protocol error"), {}};
    }
    if (!ok)
        return std::nullopt;
    buffer_.erase(0, pos);
    return out;
}

// 与上面的区别是:返回结果会保存原始RESP数据
std::optional<std::pair<RespValue, std::string>> RespParser::tryParseOneWithRaw() {
    if (buffer_.empty())
        return std::nullopt;
    size_t pos = 0;
    char prefix = buffer_[pos++];
    RespValue out;
    bool ok = false;
    switch (prefix) {
    case '+':
        ok = parseSimple(pos, RespType::kSimpleString, out);
        break;
    case '-':
        ok = parseSimple(pos, RespType::kError, out);
        break;
    case ':': {
        int64_t v = 0;
        ok = parseInteger(pos, v);
        if (ok) {
            out.type = RespType::kInteger;
            out.bulk = std::to_string(v);
        }
        break;
    }
    case '$':
        ok = parseBulkString(pos, out);
        break;
    case '*':
        ok = parseArray(pos, out);
        break;
    default:
        return std::make_optional(std::make_pair(
            RespValue{RespType::kError, std::string("protocol error"), {}},
            std::string()));
    }
    if (!ok)
        return std::nullopt;
    std::string raw(buffer_.data(), pos);
    buffer_.erase(0, pos);
    return std::make_pair(std::move(out), std::move(raw));
}


// 以下为各个类型按RESP协议进行序列化的函数实现
std::string respSimpleString(std::string_view s) {
    return std::string("+") + std::string(s) + "\r\n";
}

std::string respError(std::string_view s) {
    return std::string("-") + std::string(s) + "\r\n";
}

std::string respBulk(std::string_view s) {
    return "$" + std::to_string(s.size()) + "\r\n" + std::string(s) + "\r\n";
}

std::string respNullBulk() { return "$-1\r\n"; }
std::string respInteger(int64_t v) { return ":" + std::to_string(v) + "\r\n"; }

} // namespace mini_redis
