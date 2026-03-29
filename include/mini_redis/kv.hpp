
#pragma once

#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

namespace mini_redis {

// 自定义String类型
struct ValueRecord {
    std::string value;
    int64_t expire_at_ms = -1; //-1表示永不过期
};

// 自定义Hash类型
struct HashRecord {
    std::unordered_map<std::string, std::string> fields;
    int64_t expire_at_ms = -1;
};

// 跳表节点
struct SkiplistNode;

// 跳表
struct Skiplist {
    Skiplist(); 
    ~Skiplist();
    bool insert(double score, const std::string &member);   //插入元素至跳表
    bool erase(double score, const std::string &member);    //从跳表中删除元素
    void rangeByRank(int64_t start, int64_t stop, std::vector<std::string> &out) const; //按“排名区间”查询
    void toVector(std::vector<std::pair<double, std::string>> &out) const;  //导出整个跳表为toVector
    size_t size() const { return length_; }     //返回跳表中元素个数

private:
    int randomLevel();
    static constexpr int kMaxLevel = 32;    //最大层数上限
    static constexpr double kProbability = 0.25;    //层数提升概率
    SkiplistNode *head_;    //跳表的头结点
    int level_;     //跳表的层数
    size_t length_; //跳表的元素数量
};


// 自定义ZSet类型
struct ZSetRecord {
    // 小集合使用vector；超过阈值转为skiplist
    bool use_skiplist = false;
    std::vector<std::pair<double, std::string>> items; // 当use_skiplist=false使用，
    std::unique_ptr<Skiplist> sl; // 当use_skiplist=true使用
    //用于快速查找member的存在性O(1)，并快速获取成员的当前score（避免查vector或skiplist）
    std::unordered_map<std::string, double> member_to_score;   //集合无重复性，故member是唯一的
    int64_t expire_at_ms = -1;
};

// key-vlaue键值存储类，内部维护了三个map（String/Hash/ZSet）以及过期时间索引
class KeyValueStore {
public:
    // =====Sting API==================
    bool set(const std::string &key, const std::string &value, std::optional<int64_t> ttl_ms = std::nullopt);
    std::optional<std::string> get(const std::string &key);
    int del(const std::vector<std::string> &keys);
    bool exists(const std::string &key);
    bool setWithExpireAtMs(const std::string &key, const std::string &value, int64_t expire_at_ms);
    bool expire(const std::string &key, int64_t ttl_seconds);
    int64_t ttl(const std::string &key);
    size_t size() const { return map_.size(); }
    int expireScanStep(int max_steps);

    // 把对应String类型map_中的所有KV数据复制一份出来，返回给调用者
    std::vector<std::pair<std::string, ValueRecord>> snapshot() const;
    // 把对应hash类型map_中的所有KV数据复制一份出来，返回给调用者
    std::vector<std::pair<std::string, HashRecord>> snapshotHash() const;

    // 作为ZSet类型复制的返回类型
    struct ZSetFlat {
        std::string key;
        std::vector<std::pair<double, std::string>> items;
        int64_t expire_at_ms;
    };
    // 把对应ZSet类型map_中的所有KV数据复制一份出来，返回给调用者
    std::vector<ZSetFlat> snapshotZSet() const;
    // 获取当前数据库中所有key的函数（跨多种数据结构），并且会去重。
    std::vector<std::string> listKeys() const; 

    //===== Hash APIs =====================
    // 设置hash的filed的值，新增返回1，重设返回0
    int hset(const std::string &key, const std::string &field, const std::string &value);
    // 查找hash的某个filed的值，查找失败，则返回std::nullopt
    std::optional<std::string> hget(const std::string &key, const std::string &field);
    // 删除hash的某些filed的值，返回删除的数量
    int hdel(const std::string &key, const std::vector<std::string> &fields);
    // 判断hash的某个filed是否存在
    bool hexists(const std::string &key, const std::string &field);
    // hash里的所有field 和value 取出来展平，返回一个存放这些元素的数组
    std::vector<std::string> hgetallFlat(const std::string &key);
    // 返回hash表中的元素个数
    int hlen(const std::string &key);
    // 设置hash的过期时间
    bool setHashExpireAtMs(const std::string &key, int64_t expire_at_ms);

    // ====ZSet APIs ====================
    // 添加member，成功返回1
    int zadd(const std::string &key, double score, const std::string &member);
    // 删除members，返回成功删除的元素给书
    int zrem(const std::string &key, const std::vector<std::string> &members);
    // 返回指定范围内的members
    std::vector<std::string> zrange(const std::string &key, int64_t start, int64_t stop);
    // 返回member的score
    std::optional<double> zscore(const std::string &key, const std::string &member);
    // 设置member的过期时间
    bool setZSetExpireAtMs(const std::string &key, int64_t expire_at_ms);

private:
    // 当前时间戳
    static int64_t nowMs();
    // 检查类型是否过期
    static bool isExpired(const ValueRecord &r, int64_t now_ms);
    static bool isExpired(const HashRecord &r, int64_t now_ms);
    static bool isExpired(const ZSetRecord &r, int64_t now_ms);

    // 对某种类型的key进行检查，过期则清除
    void cleanupIfExpired(const std::string &key, int64_t now_ms);
    void cleanupIfExpiredHash(const std::string &key, int64_t now_ms);
    void cleanupIfExpiredZSet(const std::string &key, int64_t now_ms);
    // 跳表化的阈值
    static constexpr size_t kZsetVectorThreshold = 128;

private:
    //存放不同类型的键值对数据（主要是value类型的区别）
    std::unordered_map<std::string, ValueRecord> map_;  
    std::unordered_map<std::string, HashRecord> hmap_;
    std::unordered_map<std::string, ZSetRecord> zmap_;

    //一个hash表（key->过期时间），统一管理所有设置了过期时间的 key
    std::unordered_map<std::string, int64_t> expire_index_; 
    // 一个互斥锁，保证多线程访问数据时不会冲突
    // mutable：即使在const函数中，也允许修改这个成员
    mutable std::mutex mu_; 
};

} // namespace mini_redis
