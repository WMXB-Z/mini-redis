#include "mini_redis/kv.hpp"

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <iterator>
#include <optional>

namespace mini_redis {

// ---------------- Skiplist的实现 -----------------
struct SkiplistNode {
    double score;           //排序依据
    std::string member;     //存储的元素
    // 当前节点在第 i 层指向的“下一个节点”
    // node->forward[i]，在第i层链表中，紧挨着node右边的那个节点
    std::vector<SkiplistNode *> forward;    
    SkiplistNode(int level, double sc, const std::string &mem) : forward(static_cast<size_t>(level), nullptr), score(sc), member(mem) {
    }
};
Skiplist::Skiplist() : head_(new SkiplistNode(kMaxLevel, 0.0, "")), level_(1), length_(0) {}

// 依次删除第 0 层包含所有节点（即删除完整链表）
Skiplist::~Skiplist() {
    SkiplistNode *cur = head_->forward[0];
    while (cur) {
        SkiplistNode *nxt = cur->forward[0];
        delete cur;
        cur = nxt;
    }
    delete head_;
}

// 随机生成节点层数（跳表核心），高层节点稀疏，查找复杂度 O(log n)
int Skiplist::randomLevel() {
    int lvl = 1;
    // (std::rand() & 0xFFFF)：取随机数低16位（0~65535）
    while ((std::rand() & 0xFFFF) < static_cast<int>(kProbability * 0xFFFF) && lvl < kMaxLevel) {
        ++lvl;
    }
    return lvl;
}

// 定义跳表的排序规则
static inline bool less_score_member(double a_sc, const std::string &a_m, double b_sc, const std::string &b_m) {
    if (a_sc != b_sc)
        return a_sc < b_sc;
    return a_m < b_m;
}

// 找到每一层的插入位置（update[]），随机生成节点层数，在每一层插入节点（改指针）
bool Skiplist::insert(double score, const std::string &member) {
    // 第i层中，如果要插入，则这位置的前驱节点是update[i]
    // 作用：存放每一层你找到的插入点的“前一个节点”
    std::vector<SkiplistNode *> update(static_cast<size_t>(kMaxLevel));

    SkiplistNode *x = head_;
    // 从最高层开始查找插入位置（可能会更新多层）
    for (int i = level_ - 1; i >= 0; --i) {
        // 在第i层：一直往右走，直到不能再走（即找到插入点）
        while (x->forward[static_cast<size_t>(i)] &&
               less_score_member(x->forward[static_cast<size_t>(i)]->score,
                                 x->forward[static_cast<size_t>(i)]->member,
                                 score, member)) {
            x = x->forward[static_cast<size_t>(i)];
        }
        update[static_cast<size_t>(i)] = x;
    }

    x = x->forward[0];
    if (x && x->score == score && x->member == member) {    //检查元素是否已经存在
        return false; // existed
    }
    int lvl = randomLevel();    //随机生成新节点要处于的层
    // 如果突破新高，则要进行扩展
    if (lvl > level_) {
        for (int i = level_; i < lvl; ++i)
            update[static_cast<size_t>(i)] = head_;
        level_ = lvl;
    }
    // nx为待插入的节点
    SkiplistNode *nx = new SkiplistNode(lvl, score, member);
    for (int i = 0; i < lvl; ++i) {
        nx->forward[static_cast<size_t>(i)] = update[static_cast<size_t>(i)]->forward[static_cast<size_t>(i)];
        update[static_cast<size_t>(i)]->forward[static_cast<size_t>(i)] = nx;
    }
    ++length_;
    return true;
}

// 找到每一层的删除位置（update[]），逐层删除该节点，重设跳表层高
bool Skiplist::erase(double score, const std::string &member) {
    // 第i层中，如果要删除，则这位置的前驱节点是update[i]
    // 作用：存放每一层你找到的删除点的“前一个节点”
    std::vector<SkiplistNode *> update(static_cast<size_t>(kMaxLevel));
    SkiplistNode *x = head_;
    for (int i = level_ - 1; i >= 0; --i) {
        while (x->forward[static_cast<size_t>(i)] &&
               less_score_member(x->forward[static_cast<size_t>(i)]->score,
                                 x->forward[static_cast<size_t>(i)]->member,
                                 score, member)) {
            x = x->forward[static_cast<size_t>(i)];
        }
        update[static_cast<size_t>(i)] = x;
    }
    // 原本x为待删除节点的前一个节点。现在后置一位，使得x为待删除的节点
    x = x->forward[0];
    if (!x || x->score != score || x->member != member)
        return false;

    for (int i = 0; i < level_; ++i) {
        if (update[static_cast<size_t>(i)]->forward[static_cast<size_t>(i)] == x) {
            update[static_cast<size_t>(i)]->forward[static_cast<size_t>(i)] = x->forward[static_cast<size_t>(i)];
        }
    }
    delete x;
    // 如果跳表的>1的层中只有头结点， 则说明该层已经不具备查找能力，直接删除这层即可
    while (level_ > 1 && head_->forward[static_cast<size_t>(level_ - 1)] == nullptr) {
        --level_;
    }
    --length_;
    return true;
}

// 将跳表中指定范围内的元素通过vector参数返回
void Skiplist::rangeByRank(int64_t start, int64_t stop, std::vector<std::string> &out) const {
    if (length_ == 0)
        return;
    int64_t n = static_cast<int64_t>(length_);
    // auto norm = [&](int64_t idx) {
    //     if (idx < 0)
    //         idx = n + idx;
    //     if (idx < 0)
    //         idx = 0;
    //     if (idx >= n)
    //         idx = n - 1;
    //     return idx;
    // };
    // 将索引规范化：支持负索引并裁剪越界
    auto norm = [&](int64_t idx) -> int64_t {
        // 支持负索引，-1表示最后一个元素
        if (idx < 0) idx += n;
        if (idx < 0) idx = 0;       // // 越界裁剪，下界
        else if (idx >= n) idx = n - 1; // 上界
        return idx;
    };

    int64_t s = norm(start), e = norm(stop);
    if (s > e)
        return;
    // walk from head level 0
    SkiplistNode *x = head_->forward[0];
    int64_t rank = 0;
    while (x && rank < s) {
        x = x->forward[0];
        ++rank;
    }
    while (x && rank <= e) {
        out.push_back(x->member);
        x = x->forward[0];
        ++rank;
    }
}

// 将跳表转为vector，即将跳表中元素依次放入vector中，并返回这个vector
void Skiplist::toVector(std::vector<std::pair<double, std::string>> &out) const {
    out.clear();
    out.reserve(length_);
    SkiplistNode *x = head_->forward[0];
    while (x) {
        out.emplace_back(x->score, x->member);
        x = x->forward[0];
    }
}

// ---------------- KeyValueStore 的实现 -----------------
// 返回当前时间戳（毫秒）
int64_t KeyValueStore::nowMs() {
    using namespace std::chrono;
    return duration_cast<milliseconds>(steady_clock::now().time_since_epoch()).count();
}

// 判断 key 是否过期（不同类型分别处理）
bool KeyValueStore::isExpired(const ValueRecord &r, int64_t now_ms) {
    return r.expire_at_ms >= 0 && now_ms >= r.expire_at_ms;
}
bool KeyValueStore::isExpired(const HashRecord &r, int64_t now_ms) {
    return r.expire_at_ms >= 0 && now_ms >= r.expire_at_ms;
}
bool KeyValueStore::isExpired(const ZSetRecord &r, int64_t now_ms) {
    return r.expire_at_ms >= 0 && now_ms >= r.expire_at_ms;
}

// 如果key已过期，清理它。（不同类型分别处理）
void KeyValueStore::cleanupIfExpired(const std::string &key, int64_t now_ms) {
    auto it = map_.find(key);
    if (it == map_.end())   //表示没找到
        return;
    if (isExpired(it->second, now_ms)) {
        map_.erase(it);
        expire_index_.erase(key);
    }
}
void KeyValueStore::cleanupIfExpiredHash(const std::string &key, int64_t now_ms) {
    auto it = hmap_.find(key);
    if (it == hmap_.end())
        return;
    if (isExpired(it->second, now_ms)) {
        hmap_.erase(it);
        expire_index_.erase(key);
    }
}
void KeyValueStore::cleanupIfExpiredZSet(const std::string &key, int64_t now_ms) {
    auto it = zmap_.find(key);
    if (it == zmap_.end())
        return;
    if (isExpired(it->second, now_ms)) {
        zmap_.erase(it);
        expire_index_.erase(key);
    }
}


// 给对应key的String设置值或过期时间（ttl_ms为可选项）。
// 返回 true/false 表示是否设置成功。
bool KeyValueStore::set(const std::string &key, const std::string &value, std::optional<int64_t> ttl_ms) {
    std::lock_guard<std::mutex> lk(mu_);
    int64_t expire_at = -1;
    if (ttl_ms.has_value()) {
        expire_at = nowMs() + *ttl_ms;
    }
    map_[key] = ValueRecord{value, expire_at};
    if (expire_at >= 0)
        expire_index_[key] = expire_at;
    else
        expire_index_.erase(key);
    return true;
}

// 按绝对时间（毫秒）为对应key的String类型设置过期时间，而不是相对 TTL。expire_at_ms 是一个时间戳（epoch ms）。
bool KeyValueStore::setWithExpireAtMs(const std::string &key, const std::string &value, int64_t expire_at_ms) {
    std::lock_guard<std::mutex> lk(mu_);
    map_[key] = ValueRecord{value, expire_at_ms};
    if (expire_at_ms >= 0) {
        expire_index_[key] = expire_at_ms;
    }
    return true;
}

//返回对应对应key的String
std::optional<std::string> KeyValueStore::get(const std::string &key) {
    std::lock_guard<std::mutex> lk(mu_);
    int64_t now = nowMs();
    cleanupIfExpired(key, now);
    auto it = map_.find(key);
    if (it == map_.end())
        return std::nullopt;
    return it->second.value;
}

// 判断对应key的String的存在性
bool KeyValueStore::exists(const std::string &key) {
    std::lock_guard<std::mutex> lk(mu_);
    int64_t now = nowMs();
    cleanupIfExpired(key, now);
    return map_.find(key) != map_.end() || hmap_.find(key) != hmap_.end() ||
           zmap_.find(key) != zmap_.end();
}

// 删除所有对应所有key的String
int KeyValueStore::del(const std::vector<std::string> &keys) {
    std::lock_guard<std::mutex> lk(mu_);
    int removed = 0;
    int64_t now = nowMs();
    for (const auto &k : keys) {
        cleanupIfExpired(k, now);
        auto it = map_.find(k);
        if (it != map_.end()) {
            map_.erase(it);
            expire_index_.erase(k);
            ++removed;
        }
    }
    return removed;
}

// 给某个key设置过期时间（ttl_seconds是相对时间），设置成功则返回true
bool KeyValueStore::expire(const std::string &key, int64_t ttl_seconds) {
    std::lock_guard<std::mutex> lk(mu_);
    int64_t now = nowMs();
    // 如果这个key已经过期，则要先清除
    cleanupIfExpired(key, now);
    auto it = map_.find(key);
    if (it == map_.end())
        return false;
    if (ttl_seconds < 0) {
        it->second.expire_at_ms = -1;
        expire_index_.erase(key);
        return true;
    }
    it->second.expire_at_ms = now + ttl_seconds * 1000;
    expire_index_[key] = it->second.expire_at_ms;
    return true;
}

// 查询对应key的String的剩余TTL（生存时间）
int64_t KeyValueStore::ttl(const std::string &key) {
    std::lock_guard<std::mutex> lk(mu_);
    int64_t now = nowMs();
    cleanupIfExpired(key, now);
    auto it = map_.find(key);
    if (it == map_.end())
        return -2; // key does not exist
    if (it->second.expire_at_ms < 0)
        return -1; // no expire
    int64_t ms_left = it->second.expire_at_ms - now;
    if (ms_left <= 0)
        return -2;
    return ms_left / 1000; // 以秒为单位
}

// 主动过期扫描，最多扫描max_steps个 key，并删除其中已经过期的 key
int KeyValueStore::expireScanStep(int max_steps) {
    std::lock_guard<std::mutex> lk(mu_);
    if (max_steps <= 0 || expire_index_.empty())
        return 0;

    int removed = 0;
    int64_t now = nowMs();
    // random starting point
    auto it = expire_index_.begin();
    std::advance(it, static_cast<long>(std::rand() % expire_index_.size()));                          
    for (int i = 0; i < max_steps && !expire_index_.empty(); ++i) {
        if (it == expire_index_.end())
            it = expire_index_.begin();
        const std::string key = it->first;
        int64_t when = it->second;
        // when >= 0：设置了过期时间
        // now >= when：已经过期了
        if (when >= 0 && now >= when) {
            // remove from all maps
            map_.erase(key);
            hmap_.erase(key);
            zmap_.erase(key);
            it = expire_index_.erase(it);
            ++removed;
        } else {
            ++it;
        }
    }
    return removed; //返回实际移除的数量
}

// 当前键值数据库的快照拷贝函数：把当前对应类型map_中的所有KV数据复制一份出来，返回给调用者
std::vector<std::pair<std::string, ValueRecord>> KeyValueStore::snapshot() const {
    std::lock_guard<std::mutex> lk(mu_);
    std::vector<std::pair<std::string, ValueRecord>> out;
    out.reserve(map_.size());
    for (const auto &kv : map_) {
        out.emplace_back(kv.first, kv.second);
    }
    return out;
}
std::vector<std::pair<std::string, HashRecord>> KeyValueStore::snapshotHash() const {
    std::lock_guard<std::mutex> lk(mu_);
    std::vector<std::pair<std::string, HashRecord>> out;
    out.reserve(hmap_.size());
    for (const auto &kv : hmap_)
        out.emplace_back(kv.first, kv.second);
    return out;
}
std::vector<KeyValueStore::ZSetFlat> KeyValueStore::snapshotZSet() const {
    std::lock_guard<std::mutex> lk(mu_);
    std::vector<ZSetFlat> out;
    out.reserve(zmap_.size());
    for (const auto &kv : zmap_) {
        ZSetFlat flat;
        flat.key = kv.first;
        flat.expire_at_ms = kv.second.expire_at_ms;
        if (!kv.second.use_skiplist)
            flat.items = kv.second.items;
        else
            kv.second.sl->toVector(flat.items);
        out.emplace_back(std::move(flat));
    }
    return out;
}

// 获取当前数据库中所有key的函数（跨多种数据结构），并且会去重。
std::vector<std::string> KeyValueStore::listKeys() const {
    std::lock_guard<std::mutex> lk(mu_);
    std::vector<std::string> out;
    out.reserve(map_.size() + hmap_.size() + zmap_.size());
    for (const auto &kv : map_)
        out.push_back(kv.first);
    for (const auto &kv : hmap_)
        out.push_back(kv.first);
    for (const auto &kv : zmap_)
        out.push_back(kv.first);
    // 去重
    std::sort(out.begin(), out.end());

    // std::unique：把重复元素“挤到后面”，返回“去重后逻辑结尾”
    // 通过out.erase：删除真正删除尾部垃圾数据
    out.erase(std::unique(out.begin(), out.end()), out.end());
    return out;
}

// 将对应key的hash表中field对应的位置设置为value
int KeyValueStore::hset(const std::string &key, const std::string &field, const std::string &value) {
    std::lock_guard<std::mutex> lk(mu_);
    int64_t now = nowMs();
    cleanupIfExpiredHash(key, now);
    auto &rec = hmap_[key];
    auto it = rec.fields.find(field);
    if (it == rec.fields.end()) {
        rec.fields[field] = value;
        return 1;
    }
    it->second = value;
    return 0;
}

// 查找对应key的hash中filed的value
std::optional<std::string> KeyValueStore::hget(const std::string &key, const std::string &field) {
    std::lock_guard<std::mutex> lk(mu_);
    int64_t now = nowMs();
    cleanupIfExpiredHash(key, now);
    auto it = hmap_.find(key);
    if (it == hmap_.end())
        return std::nullopt;
    auto itf = it->second.fields.find(field);
    if (itf == it->second.fields.end())
        return std::nullopt;
    return itf->second;
}

// 删除对应key的hash表中数据
int KeyValueStore::hdel(const std::string &key, const std::vector<std::string> &fields) {
    std::lock_guard<std::mutex> lk(mu_);
    int64_t now = nowMs();
    cleanupIfExpiredHash(key, now);
    auto it = hmap_.find(key);
    if (it == hmap_.end())
        return 0;
    int removed = 0;
    for (const auto &f : fields) {
        auto itf = it->second.fields.find(f);
        if (itf != it->second.fields.end()) {
            it->second.fields.erase(itf);
            ++removed;
        }
    }
    if (it->second.fields.empty()) {
        hmap_.erase(it);
    }
    return removed; //返回成功删除的field数量
}

// 判断对应key的hash表中数据的存在性
bool KeyValueStore::hexists(const std::string &key, const std::string &field) {
    std::lock_guard<std::mutex> lk(mu_);
    int64_t now = nowMs();
    cleanupIfExpiredHash(key, now);
    auto it = hmap_.find(key);
    if (it == hmap_.end())
        return false;
    return it->second.fields.find(field) != it->second.fields.end();
}

// 类似Redis HGETALL命令的“扁平化返回版本”：即把对应key的hash里的所有field 和value 取出来，并按交替顺序放进一个数组中。
std::vector<std::string> KeyValueStore::hgetallFlat(const std::string &key) {
    std::lock_guard<std::mutex> lk(mu_);
    int64_t now = nowMs();
    cleanupIfExpiredHash(key, now);
    std::vector<std::string> out;
    auto it = hmap_.find(key);
    if (it == hmap_.end())
        return out;
    out.reserve(it->second.fields.size() * 2);
    for (const auto &kv : it->second.fields) {
        out.push_back(kv.first);
        out.push_back(kv.second);
    }
    return out;
}

// 返回对应key的hash表中的元素个数
int KeyValueStore::hlen(const std::string &key) {
    std::lock_guard<std::mutex> lk(mu_);
    int64_t now = nowMs();
    cleanupIfExpiredHash(key, now);
    auto it = hmap_.find(key);
    if (it == hmap_.end())
        return 0;
    return static_cast<int>(it->second.fields.size());
}

// 对对应key的hash类型设置过期时间
bool KeyValueStore::setHashExpireAtMs(const std::string &key, int64_t expire_at_ms) {
    std::lock_guard<std::mutex> lk(mu_);
    auto it = hmap_.find(key);
    if (it == hmap_.end())
        return false;
    it->second.expire_at_ms = expire_at_ms;
    if (expire_at_ms >= 0)
        expire_index_[key] = expire_at_ms;
    else
        expire_index_.erase(key);
    return true;
}

// 类似 Redis ZADD 的有序集合（Sorted Set）插入/更新操作，设计上用了两种底层结构：vector + 跳表（skiplist）动态切换
int KeyValueStore::zadd(const std::string &key, double score, const std::string &member) {
    std::lock_guard<std::mutex> lk(mu_);
    int64_t now = nowMs();
    cleanupIfExpiredZSet(key, now);
    auto &rec = zmap_[key];
    auto mit = rec.member_to_score.find(member);    //先查找，如果不存在则为新增，存在则为修改
    if (mit == rec.member_to_score.end()) {
        if (!rec.use_skiplist) {    //底层使用vector
            auto &vec = rec.items;
            // std::lower_bound二分查找函数，在vec这个“已排序数组”中，找到第一个 ≥ (score, member) 的位置
            auto it = std::lower_bound(vec.begin(), vec.end(), std::make_pair(score, member),
                                       [](const auto &a, const auto &b) {
                                           if (a.first != b.first)
                                               return a.first < b.first;
                                           return a.second < b.second;
                                       });

            vec.insert(it, std::make_pair(score, member));
            
            // 元素值大于阈值，触发跳表化
            if (vec.size() > kZsetVectorThreshold) {
                rec.use_skiplist = true;  
                rec.sl = std::make_unique<Skiplist>();
                for (const auto &pr : vec)
                    rec.sl->insert(pr.first, pr.second);
                // 清空并释放 rec.items 的内存，左边是一个空的临时vector调用了swap()，把它和rec.items交换
                std::vector<std::pair<double, std::string>>().swap(rec.items);
            }
        } else {    //底层使用跳表时
            rec.sl->insert(score, member);
        }
        rec.member_to_score.emplace(member, score); //如果member已存在，emplace不会覆盖.
        return 1;
    } else {
        double old = mit->second;
        if (old == score)
            return 0;

        // 如果存在的member的score和输入的scoer不同，则先删再插入（原因在于需要保证集合的有序性，故不能直接修改）
        if (!rec.use_skiplist) {
            auto &vec = rec.items;
            for (auto vit = vec.begin(); vit != vec.end(); ++vit) {
                if (vit->first == old && vit->second == member) {
                    vec.erase(vit);
                    break;
                }
            }
            auto it = std::lower_bound(vec.begin(), vec.end(),
                                       std::make_pair(score, member),
                                       [](const auto &a, const auto &b) {
                                           if (a.first != b.first)
                                               return a.first < b.first;
                                           return a.second < b.second;
                                       });
            vec.insert(it, std::make_pair(score, member));

            // 元素值大于阈值，触发跳表化
            if (vec.size() > kZsetVectorThreshold) {
                rec.use_skiplist = true;
                rec.sl = std::make_unique<Skiplist>();
                for (const auto &pr : vec)
                    rec.sl->insert(pr.first, pr.second);
                std::vector<std::pair<double, std::string>>().swap(rec.items);
            }
        } else {
            rec.sl->erase(old, member);
            rec.sl->insert(score, member);
        }
        mit->second = score;
        return 0;
    }
}

// 删除key对应的zsort中的指定成员members
int KeyValueStore::zrem(const std::string &key, const std::vector<std::string> &members) {
    std::lock_guard<std::mutex> lk(mu_);
    int64_t now = nowMs();
    cleanupIfExpiredZSet(key, now);
    auto it = zmap_.find(key);
    if (it == zmap_.end())
        return 0;
    int removed = 0;
    for (const auto &m : members) {
        auto mit = it->second.member_to_score.find(m);
        if (mit == it->second.member_to_score.end())
            continue;
        double sc = mit->second;
        it->second.member_to_score.erase(mit);

        if (!it->second.use_skiplist) {
            auto &vec = it->second.items;
            for (auto vit = vec.begin(); vit != vec.end(); ++vit) {
                if (vit->first == sc && vit->second == m) { //之所以还判断一下sc，是为了确保删除操作精确且安全
                    vec.erase(vit);
                    ++removed;
                    break;
                }
            }
        } else {
            if (it->second.sl->erase(sc, m))
                ++removed;
        }
    }

    // 如果有序集合中所有元素都被删除，则将这个有序集合删除
    if (!it->second.use_skiplist) {
        if (it->second.items.empty())
            zmap_.erase(it);
    } else {
        if (it->second.sl->size() == 0)
            zmap_.erase(it);
    }
    return removed;
}

// 返回key对应的zsort中指定范围内的所有members
std::vector<std::string> KeyValueStore::zrange(const std::string &key, int64_t start, int64_t stop) {
    std::lock_guard<std::mutex> lk(mu_);
    int64_t now = nowMs();
    cleanupIfExpiredZSet(key, now);
    std::vector<std::string> out;
    auto it = zmap_.find(key);
    if (it == zmap_.end())
        return out;
    if (!it->second.use_skiplist) {
        const auto &vec = it->second.items;
        int64_t n = static_cast<int64_t>(vec.size());
        if (n == 0)
            return out;
        // auto norm = [&](int64_t idx) {
        //     if (idx < 0)
        //         idx = n + idx;
        //     if (idx < 0)
        //         idx = 0;
        //     if (idx >= n)
        //         idx = n - 1;
        //     return idx;
        // };

        // 将索引规范化：支持负索引并裁剪越界
        auto norm = [&](int64_t idx) -> int64_t {
            // 支持负索引，-1表示最后一个元素
            if (idx < 0) idx += n;
            if (idx < 0) idx = 0;       // // 越界裁剪，下界
            else if (idx >= n) idx = n - 1; // 上界
            return idx;
        };
        int64_t s = norm(start), e = norm(stop);
        if (s > e)
            return out;
        for (int64_t i = s; i <= e; ++i)
            out.push_back(vec[static_cast<size_t>(i)].second);
    } else {
        it->second.sl->rangeByRank(start, stop, out);
    }
    return out;
}


// 返回key对应的zsort中member的socre
std::optional<double> KeyValueStore::zscore(const std::string &key, const std::string &member) {
    std::lock_guard<std::mutex> lk(mu_);
    int64_t now = nowMs();
    cleanupIfExpiredZSet(key, now);
    auto it = zmap_.find(key);
    if (it == zmap_.end())
        return std::nullopt;
    auto mit = it->second.member_to_score.find(member);
    if (mit == it->second.member_to_score.end())
        return std::nullopt;
    return mit->second;
}

// 对key对应的ZSet设置过期时间
bool KeyValueStore::setZSetExpireAtMs(const std::string &key, int64_t expire_at_ms) {
    std::lock_guard<std::mutex> lk(mu_);
    auto it = zmap_.find(key);
    if (it == zmap_.end())
        return false;
    it->second.expire_at_ms = expire_at_ms;
    if (expire_at_ms >= 0)
        expire_index_[key] = expire_at_ms;
    else
        expire_index_.erase(key);
    return true;
}

} // namespace mini_redis
