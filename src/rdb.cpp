#include "mini_redis/rdb.hpp"

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <filesystem>
#include "mini_redis/kv.hpp"
namespace mini_redis {

static std::string joinPath(const std::string &dir, const std::string &file) {
    if (dir.empty())
        return file;
    if (dir.back() == '/')
        return dir + file;
    return dir + "/" + file;
}

std::string Rdb::path() const { return joinPath(opts_.dir, opts_.filename); }

bool Rdb::save(const KeyValueStore &store, std::string &err) const {
    if (!opts_.enabled)
        return true;
    std::error_code ec;
    std::filesystem::create_directories(opts_.dir, ec);
    int fd = ::open(path().c_str(), O_CREAT | O_TRUNC | O_WRONLY, 0644);
    if (fd < 0) {
        err = "open rdb failed";
        return false;
    }
    auto snap_str = store.snapshot();
    auto snap_hash = store.snapshotHash();
    auto snap_zset = store.snapshotZSet();
    // MRDB2
    // Header: MRDB2\n
    // Strings: STR count\n then per line: klen key vlen value expire_ms\n
    // Hash: HASH count\n then per hash: klen key expire_ms num_fields\n then
    // num_fields lines: flen field vlen value\n ZSet: ZSET count\n then per
    // zset: klen key expire_ms num_items\n then num_items lines: score
    // member_len member\n
    std::string header = std::string("MRDB2\n");
    if (::write(fd, header.data(), header.size()) < 0) {
        ::close(fd);
        err = "write hdr";
        return false;
    }
    // STR section
    std::string line =
        std::string("STR ") + std::to_string(snap_str.size()) + "\n";
    if (::write(fd, line.data(), line.size()) < 0) {
        ::close(fd);
        err = "write str cnt";
        return false;
    }
    for (const auto &kv : snap_str) {
        const std::string &k = kv.first;
        const ValueRecord &r = kv.second;
        std::string rec;
        rec.append(std::to_string(k.size()))
            .append(" ")
            .append(k)
            .append(" ")
            .append(std::to_string(r.value.size()))
            .append(" ")
            .append(r.value)
            .append(" ")
            .append(std::to_string(r.expire_at_ms))
            .append("\n");
        if (::write(fd, rec.data(), rec.size()) < 0) {
            ::close(fd);
            err = "write str rec";
            return false;
        }
    }
    // HASH section
    line = std::string("HASH ") + std::to_string(snap_hash.size()) + "\n";
    if (::write(fd, line.data(), line.size()) < 0) {
        ::close(fd);
        err = "write hash cnt";
        return false;
    }
    for (const auto &kv : snap_hash) {
        const std::string &k = kv.first;
        const HashRecord &r = kv.second;
        std::string rec_head;
        rec_head.append(std::to_string(k.size()))
            .append(" ")
            .append(k)
            .append(" ")
            .append(std::to_string(r.expire_at_ms))
            .append(" ")
            .append(std::to_string(r.fields.size()))
            .append("\n");
        if (::write(fd, rec_head.data(), rec_head.size()) < 0) {
            ::close(fd);
            err = "write hash head";
            return false;
        }
        for (const auto &fv : r.fields) {
            std::string fline;
            fline.append(std::to_string(fv.first.size()))
                .append(" ")
                .append(fv.first)
                .append(" ")
                .append(std::to_string(fv.second.size()))
                .append(" ")
                .append(fv.second)
                .append("\n");
            if (::write(fd, fline.data(), fline.size()) < 0) {
                ::close(fd);
                err = "write hash field";
                return false;
            }
        }
    }
    // ZSET section
    line = std::string("ZSET ") + std::to_string(snap_zset.size()) + "\n";
    if (::write(fd, line.data(), line.size()) < 0) {
        ::close(fd);
        err = "write zset cnt";
        return false;
    }
    for (const auto &flat : snap_zset) {
        const std::string &k = flat.key;
        std::string rec_head;
        rec_head.append(std::to_string(k.size()))
            .append(" ")
            .append(k)
            .append(" ")
            .append(std::to_string(flat.expire_at_ms))
            .append(" ")
            .append(std::to_string(flat.items.size()))
            .append("\n");
        if (::write(fd, rec_head.data(), rec_head.size()) < 0) {
            ::close(fd);
            err = "write zset head";
            return false;
        }
        for (const auto &it : flat.items) {
            const double score = it.first;
            const std::string &member = it.second;
            std::string iline;
            iline.append(std::to_string(score))
                .append(" ")
                .append(std::to_string(member.size()))
                .append(" ")
                .append(member)
                .append("\n");
            if (::write(fd, iline.data(), iline.size()) < 0) {
                ::close(fd);
                err = "write zset item";
                return false;
            }
        }
    }
    ::fsync(fd);
    ::close(fd);
    return true;
}

bool Rdb::load(KeyValueStore &store, std::string &err) const {
    if (!opts_.enabled)
        return true;
    int fd = ::open(path().c_str(), O_RDONLY);
    if (fd < 0)
        return true; // no file is fine
    std::string data;
    data.resize(1 << 20);
    std::string file;
    while (true) {
        ssize_t r = ::read(fd, data.data(), data.size());
        if (r < 0) {
            ::close(fd);
            err = "read rdb";
            return false;
        }
        if (r == 0)
            break;
        file.append(data.data(), static_cast<size_t>(r));
    }
    ::close(fd);
    size_t pos = 0;
    auto readLine = [&](std::string &out) -> bool {
        size_t e = file.find('\n', pos);
        if (e == std::string::npos)
            return false;
        out.assign(file.data() + pos, e - pos);
        pos = e + 1;
        return true;
    };
    std::string line;
    if (!readLine(line)) {
        err = "bad magic";
        return false;
    }
    if (line == "MRDB1") {
        // backward compat for strings only
        if (!readLine(line)) {
            err = "no count";
            return false;
        }
        int count = std::stoi(line);
        for (int i = 0; i < count; ++i) {
            if (!readLine(line)) {
                err = "trunc rec";
                return false;
            }
            size_t p = 0;
            auto nextTok = [&](std::string &tok) -> bool {
                size_t s = p;
                while (s < line.size() && line[s] == ' ')
                    ++s;
                size_t q = line.find(' ', s);
                if (q == std::string::npos) {
                    tok = line.substr(s);
                    p = line.size();
                    return true;
                }
                tok = line.substr(s, q - s);
                p = q + 1;
                return true;
            };
            std::string key_len_s;
            nextTok(key_len_s);
            int klen = std::stoi(key_len_s);
            std::string key = line.substr(p, static_cast<size_t>(klen));
            p += static_cast<size_t>(klen) + 1;
            std::string val_len_s;
            nextTok(val_len_s);
            int vlen = std::stoi(val_len_s);
            std::string val = line.substr(p, static_cast<size_t>(vlen));
            p += static_cast<size_t>(vlen) + 1;
            std::string exp_s;
            nextTok(exp_s);
            int64_t exp = std::stoll(exp_s);
            store.setWithExpireAtMs(key, val, exp);
        }
        return true;
    }
    if (line != "MRDB2") {
        err = "bad magic";
        return false;
    }
    // STR section
    if (!readLine(line)) {
        err = "no str section";
        return false;
    }
    if (line.rfind("STR ", 0) != 0) {
        err = "no str tag";
        return false;
    }
    int str_count = std::stoi(line.substr(4));
    for (int i = 0; i < str_count; ++i) {
        if (!readLine(line)) {
            err = "trunc str rec";
            return false;
        }
        size_t p = 0;
        auto nextTok = [&](std::string &tok) -> bool {
            size_t s = p;
            while (s < line.size() && line[s] == ' ')
                ++s;
            size_t q = line.find(' ', s);
            if (q == std::string::npos) {
                tok = line.substr(s);
                p = line.size();
                return true;
            }
            tok = line.substr(s, q - s);
            p = q + 1;
            return true;
        };
        std::string key_len_s;
        nextTok(key_len_s);
        int klen = std::stoi(key_len_s);
        std::string key = line.substr(p, static_cast<size_t>(klen));
        p += static_cast<size_t>(klen) + 1;
        std::string val_len_s;
        nextTok(val_len_s);
        int vlen = std::stoi(val_len_s);
        std::string val = line.substr(p, static_cast<size_t>(vlen));
        p += static_cast<size_t>(vlen) + 1;
        std::string exp_s;
        nextTok(exp_s);
        int64_t exp = std::stoll(exp_s);
        store.setWithExpireAtMs(key, val, exp);
    }
    // HASH section
    if (!readLine(line)) {
        err = "no hash section";
        return false;
    }
    if (line.rfind("HASH ", 0) != 0) {
        err = "no hash tag";
        return false;
    }
    int hash_count = std::stoi(line.substr(5));
    for (int i = 0; i < hash_count; ++i) {
        if (!readLine(line)) {
            err = "trunc hash head";
            return false;
        }
        size_t p = 0;
        auto nextTok = [&](std::string &tok) -> bool {
            size_t s = p;
            while (s < line.size() && line[s] == ' ')
                ++s;
            size_t q = line.find(' ', s);
            if (q == std::string::npos) {
                tok = line.substr(s);
                p = line.size();
                return true;
            }
            tok = line.substr(s, q - s);
            p = q + 1;
            return true;
        };
        std::string klen_s;
        nextTok(klen_s);
        int klen = std::stoi(klen_s);
        std::string key = line.substr(p, static_cast<size_t>(klen));
        p += static_cast<size_t>(klen) + 1;
        std::string exp_s;
        nextTok(exp_s);
        int64_t exp = std::stoll(exp_s);
        std::string nfields_s;
        nextTok(nfields_s);
        int nf = std::stoi(nfields_s);
        bool has_any = false;
        std::vector<std::pair<std::string, std::string>> fvs;
        fvs.reserve(nf);
        for (int j = 0; j < nf; ++j) {
            if (!readLine(line)) {
                err = "trunc hash field";
                return false;
            }
            size_t q = 0;
            auto nextTok2 = [&](std::string &tok) -> bool {
                size_t s = q;
                while (s < line.size() && line[s] == ' ')
                    ++s;
                size_t x = line.find(' ', s);
                if (x == std::string::npos) {
                    tok = line.substr(s);
                    q = line.size();
                    return true;
                }
                tok = line.substr(s, x - s);
                q = x + 1;
                return true;
            };
            std::string flen_s;
            nextTok2(flen_s);
            int flen = std::stoi(flen_s);
            std::string field = line.substr(q, static_cast<size_t>(flen));
            q += static_cast<size_t>(flen) + 1;
            std::string vlen_s;
            nextTok2(vlen_s);
            int vlen = std::stoi(vlen_s);
            std::string val = line.substr(q, static_cast<size_t>(vlen));
            fvs.emplace_back(std::move(field), std::move(val));
            has_any = true;
        }
        if (has_any) {
            for (const auto &fv : fvs)
                store.hset(key, fv.first, fv.second);
            if (exp >= 0)
                store.setHashExpireAtMs(key, exp);
        }
    }
    // ZSET section
    if (!readLine(line)) {
        err = "no zset section";
        return false;
    }
    if (line.rfind("ZSET ", 0) != 0) {
        err = "no zset tag";
        return false;
    }
    int zset_count = std::stoi(line.substr(5));
    for (int i = 0; i < zset_count; ++i) {
        if (!readLine(line)) {
            err = "trunc zset head";
            return false;
        }
        size_t p = 0;
        auto nextTok = [&](std::string &tok) -> bool {
            size_t s = p;
            while (s < line.size() && line[s] == ' ')
                ++s;
            size_t q = line.find(' ', s);
            if (q == std::string::npos) {
                tok = line.substr(s);
                p = line.size();
                return true;
            }
            tok = line.substr(s, q - s);
            p = q + 1;
            return true;
        };
        std::string klen_s;
        nextTok(klen_s);
        int klen = std::stoi(klen_s);
        std::string key = line.substr(p, static_cast<size_t>(klen));
        p += static_cast<size_t>(klen) + 1;
        std::string exp_s;
        nextTok(exp_s);
        int64_t exp = std::stoll(exp_s);
        std::string nitems_s;
        nextTok(nitems_s);
        int ni = std::stoi(nitems_s);
        for (int j = 0; j < ni; ++j) {
            if (!readLine(line)) {
                err = "trunc zset item";
                return false;
            }
            size_t q = 0;
            auto nextTok2 = [&](std::string &tok) -> bool {
                size_t s = q;
                while (s < line.size() && line[s] == ' ')
                    ++s;
                size_t x = line.find(' ', s);
                if (x == std::string::npos) {
                    tok = line.substr(s);
                    q = line.size();
                    return true;
                }
                tok = line.substr(s, x - s);
                q = x + 1;
                return true;
            };
            std::string score_s;
            nextTok2(score_s);
            double sc = std::stod(score_s);
            std::string mlen_s;
            nextTok2(mlen_s);
            int ml = std::stoi(mlen_s);
            std::string member = line.substr(q, static_cast<size_t>(ml));
            store.zadd(key, sc, member);
        }
        if (exp >= 0)
            store.setZSetExpireAtMs(key, exp);
    }
    return true;
}

} // namespace mini_redis
