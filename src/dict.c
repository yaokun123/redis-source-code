#include "fmacros.h"

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <stdarg.h>
#include <limits.h>
#include <sys/time.h>

#include "dict.h"
#include "zmalloc.h"
#ifndef DICT_BENCHMARK_MAIN
#include "redisassert.h"
#else
#include <assert.h>
#endif
/**
 * 通过 dictEnableResize() 和 dictDisableResize() 两个函数，程序可以手动地允许或阻止哈希表进行 rehash ，
 * 这在 Redis 使用子进程进行保存操作时，可以有效地利用 copy-on-write 机制。
 * 需要注意的是，并非所有 rehash 都会被 dictDisableResize 阻止：如果已使用节点的数量和字典大小之间的比率，
 * 大于字典强制 rehash 比率 dict_force_resize_ratio ，那么 rehash 仍然会（强制）进行。
 * */
//// 指示字典是否启用 rehash 的标识
static int dict_can_resize = 1;
static unsigned int dict_force_resize_ratio = 5;                // 负载因子

/* -------------------------- private prototypes ---------------------------- */

static int _dictExpandIfNeeded(dict *ht);
static unsigned long _dictNextPower(unsigned long size);
static int _dictKeyIndex(dict *ht, const void *key, unsigned int hash, dictEntry **existing);
static int _dictInit(dict *ht, dictType *type, void *privDataPtr);

/* -------------------------- hash functions -------------------------------- */

static uint8_t dict_hash_function_seed[16];

void dictSetHashFunctionSeed(uint8_t *seed) {
    memcpy(dict_hash_function_seed,seed,sizeof(dict_hash_function_seed));
}

uint8_t *dictGetHashFunctionSeed(void) {
    return dict_hash_function_seed;
}

/* The default hashing function uses SipHash implementation
 * in siphash.c. */

uint64_t siphash(const uint8_t *in, const size_t inlen, const uint8_t *k);
uint64_t siphash_nocase(const uint8_t *in, const size_t inlen, const uint8_t *k);

uint64_t dictGenHashFunction(const void *key, int len) {
    return siphash(key,len,dict_hash_function_seed);
}

uint64_t dictGenCaseHashFunction(const unsigned char *buf, int len) {
    return siphash_nocase(buf,len,dict_hash_function_seed);
}

/* ----------------------------- API implementation ------------------------- */

//// 重置哈希表
static void _dictReset(dictht *ht)
{
    ht->table = NULL;
    ht->size = 0;
    ht->sizemask = 0;
    ht->used = 0;
}

//// 创建一个空字典
dict *dictCreate(dictType *type,void *privDataPtr)
{
    dict *d = zmalloc(sizeof(*d));

    _dictInit(d,type,privDataPtr);
    return d;
}

//// 初始化字典
int _dictInit(dict *d, dictType *type,void *privDataPtr)
{
    // 初始化两个哈希表的各项属性值
    // 但暂时还不分配内存给哈希表数组
    _dictReset(&d->ht[0]);
    _dictReset(&d->ht[1]);


    d->type = type;                 // 设置类型特定函数
    d->privdata = privDataPtr;
    d->rehashidx = -1;              // 设置哈希表 rehash 状态。-1，代表没有在rehash
    d->iterators = 0;               // 设置字典的安全迭代器数量
    return DICT_OK;
}


//// 字典resize
int dictResize(dict *d)
{
    int minimal;

    // 不能resize 或 正在rehash直接返回，dict_can_resize为全局变量，有fork进程时候会设置为0，优化写时拷贝
    if (!dict_can_resize || dictIsRehashing(d)) return DICT_ERR;

    // 根据ht[0].used（哈希表中节点个数）重新计算数组大小
    minimal = d->ht[0].used;                // 元素个数
    if (minimal < DICT_HT_INITIAL_SIZE)     // 如果元素个数小于4，最小size就按照4来算
        minimal = DICT_HT_INITIAL_SIZE;
    return dictExpand(d, minimal);
}


//// 创建一个新的哈希表，并根据字典的情况，选择以下其中一个动作来进行：
//// 1) 如果字典的 0 号哈希表为空，那么将新哈希表设置为 0 号哈希表
//// 2) 如果字典的 0 号哈希表非空，那么将新哈希表设置为 1 号哈希表，并打开字典的 rehash 标识，使得程序可以开始对字典进行 rehash
int dictExpand(dict *d, unsigned long size)
{
    dictht n;
    unsigned long realsize = _dictNextPower(size);              // 计算新的hash table数组容量，2的倍数

    //// 正在rehash直接返回，或传入的size小于节点个数也返回
    if (dictIsRehashing(d) || d->ht[0].used > size)
        return DICT_ERR;

    if (realsize == d->ht[0].size) return DICT_ERR;             // 新的容量与旧容量相同，直接返回

    // 分配新的hash table
    n.size = realsize;
    n.sizemask = realsize-1;
    n.table = zcalloc(realsize*sizeof(dictEntry*));
    n.used = 0;

    if (d->ht[0].table == NULL) {// 如果hash table是第一次设置，直接返回
        d->ht[0] = n;
        return DICT_OK;
    }

    d->ht[1] = n;               // 将新分配的hash table分配给ht[1]
    d->rehashidx = 0;           // 设置rehash的索引
    return DICT_OK;
}


//// 渐进式rehash
// 执行N步渐进式的rehash操作，如果仍存在旧表中的数据迁移到新表，则返回1，反之返回0
// 每一步操作移动一个索引值下的键值对到新表
int dictRehash(dict *d, int n) {
    int empty_visits = n*10;                                        // 最大允许访问的空桶值，也就是该索引下没有键值对
    if (!dictIsRehashing(d)) return 0;                              // 检查当前字典是否在进行rehash操作

    while(n-- && d->ht[0].used != 0) {
        dictEntry *de, *nextde;

        // rehashidx不能大于哈希表的大小
        assert(d->ht[0].size > (unsigned long)d->rehashidx);
        while(d->ht[0].table[d->rehashidx] == NULL) {               // 空hash槽的处理逻辑
            d->rehashidx++;
            if (--empty_visits == 0) return 1;
        }
        de = d->ht[0].table[d->rehashidx];                          // hash槽的入口

        // 循环将哈希冲突的链表上的节点全部转移走
        while(de) {                                                 // 将hash槽里所有的节点都转移到新的hash表上
            unsigned int h;

            nextde = de->next;
            h = dictHashKey(d, de->key) & d->ht[1].sizemask;    // 计算该键值对在新表中的索引值

            // 头插到ht[1][h]中
            de->next = d->ht[1].table[h];
            d->ht[1].table[h] = de;

            // 数量操作
            d->ht[0].used--;
            d->ht[1].used++;
            de = nextde;
        }
        d->ht[0].table[d->rehashidx] = NULL;
        d->rehashidx++;
    }

    // 键值是否整个表都迁移完成
    if (d->ht[0].used == 0) {
        zfree(d->ht[0].table);                                      // 清除ht[0]
        d->ht[0] = d->ht[1];                                        // 将ht[1]转移到ht[0]
        _dictReset(&d->ht[1]);                                  // 重置ht[1]为空哈希表
        d->rehashidx = -1;                                         // 完成rehash，-1代表没有进行rehash操作
        return 0;
    }

    return 1;                                                       // 如果没有完成rehash则返回1
}

//// 获取当前的时间戳（以毫秒为单位）
long long timeInMilliseconds(void) {
    struct timeval tv;

    gettimeofday(&tv,NULL);
    return (((long long)tv.tv_sec)*1000)+(tv.tv_usec/1000);
}

//// rehash操作每次执行{ms}时间就退出。每次执行100步
int dictRehashMilliseconds(dict *d, int ms) {
    long long start = timeInMilliseconds();
    int rehashes = 0;

    while(dictRehash(d,100)) {                                  // 每次执行100步
        rehashes += 100;
        if (timeInMilliseconds()-start > ms) break;               // 如果时间超过ms就退出
    }
    return rehashes;
}

//// 在执行查询和更新操作时，如果符合rehash条件就会触发一次rehash操作，每次执行1步
//// 前提是当前没有正在使用的迭代器
static void _dictRehashStep(dict *d) {
    if (d->iterators == 0) dictRehash(d,1);
}

//// 添加键值对
//// 为什么设置键和值要分开呢？是因为要先查询键是否存在，如果存在就覆盖值，不存在才新建
int dictAdd(dict *d, void *key, void *val)
{
    // 返回的entry可能是已经存在的节点，也可能是新建的节点
    //// 这里existing设置的是NULL，代表该方法只做新增，不做更新（如果key存在就直接返回NULL节点）
    dictEntry *entry = dictAddRaw(d,key,NULL);       // 往字典中添加一个只有key的键值对

    if (!entry) return DICT_ERR;                            // 如果添加失败，则返回错误
    dictSetVal(d, entry, val);                              // 为添加的只有key键值对设定值
    return DICT_OK;
}

//// 如果此时没有进行rehash操作，直接计算出索引添加到ht[0]中
//// 如果此刻正在进行rehash操作，则根据ht[1]的参数计算出索引值，添加到ht[1]中
dictEntry *dictAddRaw(dict *d, void *key, dictEntry **existing)
{
    int index;
    dictEntry *entry;
    dictht *ht;

    if (dictIsRehashing(d)) _dictRehashStep(d);                             // 如果正在进行rehash操作，则先执行一下rehash操作

    //// 比较key的时候，如果初始化的时候有设置keyCompare那么就调用keyCompare函数，如果没有设置比较的就是地址
    if ((index = _dictKeyIndex(d, key, dictHashKey(d,key), existing)) == -1)// 判断key是否在字典中存在，存在返回-1
        return NULL;

    ht = dictIsRehashing(d) ? &d->ht[1] : &d->ht[0];                        // 如果正在进行rehash则添加到ht[1]，反之则添加到ht[0]
    entry = zmalloc(sizeof(*entry));                                        // 申请内存，存储新键值对
    entry->next = ht->table[index];                                         // 单链表，头插法
    ht->table[index] = entry;
    ht->used++;

    /* Set the hash entry fields. */
    dictSetKey(d, entry, key);                                              // 给节点设定键
    return entry;
}

//// 上述添加方式dictAdd，在存在该key的时候，直接返回NULL，Redis还提供了另一种添加键值对的函数，
//// 它在处理存在相同key的情况时，直接用新键值对来替换旧键值对。
int dictReplace(dict *d, void *key, void *val)
{
    dictEntry *entry, *existing, auxentry;

    entry = dictAddRaw(d,key,&existing);        // 如果存在相同的key，则先获取该键值对
    if (entry) {// 说明key不存在，直接设置val即可
        dictSetVal(d, entry, val);
        return 1;
    }

    //如果key存在，则要覆盖value
    auxentry = *existing;
    dictSetVal(d, existing, val);
    dictFreeVal(d, &auxentry);
    return 0;
}

//// 根据key在字典中 添加 / 查找
dictEntry *dictAddOrFind(dict *d, void *key) {
    dictEntry *entry, *existing;
    entry = dictAddRaw(d,key,&existing);
    return entry ? entry : existing;
}

//// 查找并删除指定键对应的键值对
static dictEntry *dictGenericDelete(dict *d, const void *key, int nofree) {
    unsigned int h, idx;
    dictEntry *he, *prevHe;
    int table;

    if (d->ht[0].used == 0 && d->ht[1].used == 0) return NULL;              // 字典为空

    if (dictIsRehashing(d)) _dictRehashStep(d);                             // 如果正在进行rehash，则出发一次rehash操作
    h = dictHashKey(d, key);                                                // 计算哈希值

    for (table = 0; table <= 1; table++) {                                  // 在两个table中查找
        idx = h & d->ht[table].sizemask;
        he = d->ht[table].table[idx];
        prevHe = NULL;
        while(he) {
            if (key==he->key || dictCompareKeys(d, key, he->key)) {
                //// 找到节点，要删除
                if (prevHe)                                                 // 如果不是第一个节点
                    prevHe->next = he->next;
                else                                                        // 如果是第一个节点
                    d->ht[table].table[idx] = he->next;

                //// 是否释放这个节点
                if (!nofree) {
                    dictFreeKey(d, he);
                    dictFreeVal(d, he);
                    zfree(he);
                }
                d->ht[table].used--;                                        // 数量减1
                return he;
            }
            prevHe = he;
            he = he->next;
        }
        if (!dictIsRehashing(d)) break;                                 // 如果没有进行rehash操作，则没必要对ht[1]进行查找
    }
    return NULL; /* not found */
}

//// 删除该键值对，并释放键和值
int dictDelete(dict *ht, const void *key) {
    return dictGenericDelete(ht,key,0) ? DICT_OK : DICT_ERR;
}

//// 删除该键值对，不释放键和值
dictEntry *dictUnlink(dict *ht, const void *key) {
    return dictGenericDelete(ht,key,1);
}

//// 释放一个节点
void dictFreeUnlinkedEntry(dict *d, dictEntry *he) {
    if (he == NULL) return;
    dictFreeKey(d, he);
    dictFreeVal(d, he);
    zfree(he);
}

//// 释放哈希表
int _dictClear(dict *d, dictht *ht, void(callback)(void *)) {
    unsigned long i;

    for (i = 0; i < ht->size && ht->used > 0; i++) {                // 释放每一个哈希槽
        dictEntry *he, *nextHe;

        if (callback && (i & 65535) == 0) callback(d->privdata);

        if ((he = ht->table[i]) == NULL) continue;
        while(he) {                                                 // 释放哈希槽下面存放的所有节点
            nextHe = he->next;
            dictFreeKey(d, he);                                     // 释放键
            dictFreeVal(d, he);                                     // 释放值
            zfree(he);
            ht->used--;
            he = nextHe;
        }
    }
    zfree(ht->table);                                               // 释放哈希表
    _dictReset(ht);                                                 // 重置哈希表
    return DICT_OK;
}

//// 字典删除
void dictRelease(dict *d)
{
    _dictClear(d,&d->ht[0],NULL);   // 清除哈希表ht[0]
    _dictClear(d,&d->ht[1],NULL);   // 清除哈希表ht[1]
    zfree(d);                                  // 释放字典
}


//// 根据键在字典中查找对应的节点
dictEntry *dictFind(dict *d, const void *key)
{
    dictEntry *he;
    unsigned int h, idx, table;

    if (d->ht[0].used + d->ht[1].used == 0) return NULL;                // 字典为空，返回NULL
    if (dictIsRehashing(d)) _dictRehashStep(d);                         // 如果正在进行rehash，则执行rehash操作
    h = dictHashKey(d, key);                                            // 计算哈希值
    for (table = 0; table <= 1; table++) {                              // 在两个表中查找对应的键值对
        idx = h & d->ht[table].sizemask;
        he = d->ht[table].table[idx];
        while(he) {                                                     // 如果hash冲突，会遍历单链表
            if (key==he->key || dictCompareKeys(d, key, he->key))
                return he;
            he = he->next;
        }
        if (!dictIsRehashing(d)) return NULL;                           // 如果没有进行rehash，则直接返回，不再ht[1]中查找
    }
    return NULL;
}

//// 根据键在字典中查找对应的节点值
void *dictFetchValue(dict *d, const void *key) {
    dictEntry *he;

    he = dictFind(d,key);
    return he ? dictGetVal(he) : NULL;
}

//// 字典状态展示
long long dictFingerprint(dict *d) {
    long long integers[6], hash = 0;
    int j;

    integers[0] = (long) d->ht[0].table;
    integers[1] = d->ht[0].size;            // ht[0]数组大小
    integers[2] = d->ht[0].used;            // ht[0]元素个数

    integers[3] = (long) d->ht[1].table;
    integers[4] = d->ht[1].size;            // ht[1]数组大小
    integers[5] = d->ht[1].used;            // ht[1]元素个数


    for (j = 0; j < 6; j++) {
        hash += integers[j];
        /* For the hashing step we use Tomas Wang's 64 bit integer hash. */
        hash = (~hash) + (hash << 21); // hash = (hash << 21) - hash - 1;
        hash = hash ^ (hash >> 24);
        hash = (hash + (hash << 3)) + (hash << 8); // hash * 265
        hash = hash ^ (hash >> 14);
        hash = (hash + (hash << 2)) + (hash << 4); // hash * 21
        hash = hash ^ (hash >> 28);
        hash = hash + (hash << 31);
    }
    return hash;
}

//// 获取一个字典迭代器
dictIterator *dictGetIterator(dict *d)
{
    dictIterator *iter = zmalloc(sizeof(*iter));

    iter->d = d;
    iter->table = 0;
    iter->index = -1;
    iter->safe = 0;
    iter->entry = NULL;
    iter->nextEntry = NULL;
    return iter;
}

//// 获取一个安全的字典迭代器
dictIterator *dictGetSafeIterator(dict *d) {
    dictIterator *i = dictGetIterator(d);

    i->safe = 1;
    return i;
}

//// 返回迭代器指向的当前节点，字典迭代完毕时，返回 NULL
dictEntry *dictNext(dictIterator *iter)
{
    while (1) {
        // 进入这个循环有两种可能：
        // 1) 这是迭代器第一次运行
        // 2) 当前索引链表中的节点已经迭代完（NULL 为链表的表尾）
        if (iter->entry == NULL) {
            dictht *ht = &iter->d->ht[iter->table];                 // 指向被迭代的哈希表
            if (iter->index == -1 && iter->table == 0) {            // 初次迭代时执行
                if (iter->safe)                                     // 如果是安全迭代器，那么更新安全迭代器计数器
                    iter->d->iterators++;
                else
                    iter->fingerprint = dictFingerprint(iter->d);   // 如果是不安全迭代器，那么计算指纹
            }
            iter->index++;                                          // 更新索引


            // 如果迭代器的当前索引大于当前被迭代的哈希表的大小
            // 那么说明这个哈希表已经迭代完毕
            if (iter->index >= (long) ht->size) {
                // 如果正在 rehash 的话，那么说明 1 号哈希表也正在使用中
                // 那么继续对 1 号哈希表进行迭代
                if (dictIsRehashing(iter->d) && iter->table == 0) {
                    iter->table++;
                    iter->index = 0;
                    ht = &iter->d->ht[1];
                } else {                                            // 如果没有 rehash ，那么说明迭代已经完成
                    break;
                }
            }

            // 如果进行到这里，说明这个哈希表并未迭代完
            // 更新节点指针，指向下个索引链表的表头节点
            iter->entry = ht->table[iter->index];
        } else {
            // 执行到这里，说明程序正在迭代某个链表
            // 将节点指针指向链表的下个节点
            iter->entry = iter->nextEntry;
        }


        // 如果当前节点不为空，那么也记录下该节点的下个节点
        // 因为安全迭代器有可能会将迭代器返回的当前节点删除
        if (iter->entry) {
            /* We need to save the 'next' here, the iterator user
             * may delete the entry we are returning. */
            iter->nextEntry = iter->entry->next;
            return iter->entry;
        }
    }

    // 迭代完毕
    return NULL;
}

//// 释放迭代器
void dictReleaseIterator(dictIterator *iter)
{
    if (!(iter->index == -1 && iter->table == 0)) {
        if (iter->safe)
            iter->d->iterators--;
        else
            assert(iter->fingerprint == dictFingerprint(iter->d));
    }
    zfree(iter);
}

//// 从字典中随机返回一个节点
dictEntry *dictGetRandomKey(dict *d)
{
    dictEntry *he, *orighe;
    unsigned int h;
    int listlen, listele;

    if (dictSize(d) == 0) return NULL;                                  // 哈希表为空，直接返回NULL
    if (dictIsRehashing(d)) _dictRehashStep(d);                         // 如果正在进行rehash，则执行一次rehash操作

    // 随机返回一个键的具体操作是：先随机选取一个索引值，然后再取该索引值
    // 对应的键值对链表中随机选取一个键值对返回
    if (dictIsRehashing(d)) {                                           // 如果在进行rehash，则需要考虑两个哈希表中的数据
        do {
            h = d->rehashidx + (random() % (d->ht[0].size +
                                            d->ht[1].size -
                                            d->rehashidx));
            he = (h >= d->ht[0].size) ? d->ht[1].table[h - d->ht[0].size] :
                                      d->ht[0].table[h];
        } while(he == NULL);
    } else {                                                            // 没有rehash的情况
        do {
            h = random() & d->ht[0].sizemask;                           // 获取哈希槽
            he = d->ht[0].table[h];                                     // 获取哈希槽存放的头节点
        } while(he == NULL);
    }

    // 到这里，就随机选取了一个非空的键值对链表
    // 然后随机从这个拥有相同索引值的链表中随机选取一个键值对
    listlen = 0;
    orighe = he;
    while(he) {
        he = he->next;
        listlen++;
    }
    listele = random() % listlen;
    he = orighe;
    while(listele--) he = he->next;
    return he;
}

//// 得到一些keys
unsigned int dictGetSomeKeys(dict *d, dictEntry **des, unsigned int count) {
    unsigned long j; /* internal hash table id, 0 or 1. */
    unsigned long tables; /* 1 or 2 tables? */
    unsigned long stored = 0, maxsizemask;
    unsigned long maxsteps;

    if (dictSize(d) < count) count = dictSize(d);
    maxsteps = count*10;

    /* Try to do a rehashing work proportional to 'count'. */
    for (j = 0; j < count; j++) {
        if (dictIsRehashing(d))
            _dictRehashStep(d);
        else
            break;
    }

    tables = dictIsRehashing(d) ? 2 : 1;
    maxsizemask = d->ht[0].sizemask;
    if (tables > 1 && maxsizemask < d->ht[1].sizemask)
        maxsizemask = d->ht[1].sizemask;

    /* Pick a random point inside the larger table. */
    unsigned long i = random() & maxsizemask;
    unsigned long emptylen = 0; /* Continuous empty entries so far. */
    while(stored < count && maxsteps--) {
        for (j = 0; j < tables; j++) {
            /* Invariant of the dict.c rehashing: up to the indexes already
             * visited in ht[0] during the rehashing, there are no populated
             * buckets, so we can skip ht[0] for indexes between 0 and idx-1. */
            if (tables == 2 && j == 0 && i < (unsigned long) d->rehashidx) {
                /* Moreover, if we are currently out of range in the second
                 * table, there will be no elements in both tables up to
                 * the current rehashing index, so we jump if possible.
                 * (this happens when going from big to small table). */
                if (i >= d->ht[1].size) i = d->rehashidx;
                continue;
            }
            if (i >= d->ht[j].size) continue; /* Out of range for this table. */
            dictEntry *he = d->ht[j].table[i];

            /* Count contiguous empty buckets, and jump to other
             * locations if they reach 'count' (with a minimum of 5). */
            if (he == NULL) {
                emptylen++;
                if (emptylen >= 5 && emptylen > count) {
                    i = random() & maxsizemask;
                    emptylen = 0;
                }
            } else {
                emptylen = 0;
                while (he) {
                    /* Collect all the elements of the buckets found non
                     * empty while iterating. */
                    *des = he;
                    des++;
                    he = he->next;
                    stored++;
                    if (stored == count) return stored;
                }
            }
        }
        i = (i+1) & maxsizemask;
    }
    return stored;
}

/* Function to reverse bits. Algorithm from:
 * http://graphics.stanford.edu/~seander/bithacks.html#ReverseParallel */
static unsigned long rev(unsigned long v) {
    unsigned long s = 8 * sizeof(v); // bit size; must be power of 2
    unsigned long mask = ~0;
    while ((s >>= 1) > 0) {
        mask ^= (mask << s);
        v = ((v >> s) & mask) | ((v << s) & ~mask);
    }
    return v;
}

// 扫描dict
unsigned long dictScan(dict *d,
                       unsigned long v,
                       dictScanFunction *fn,
                       dictScanBucketFunction* bucketfn,
                       void *privdata)
{
    dictht *t0, *t1;
    const dictEntry *de, *next;
    unsigned long m0, m1;

    if (dictSize(d) == 0) return 0;                             // 字典为空，直接返回

    if (!dictIsRehashing(d)) { //没有在rehash
        t0 = &(d->ht[0]);
        m0 = t0->sizemask;

        /* Emit entries at cursor */
        if (bucketfn) bucketfn(privdata, &t0->table[v & m0]);

        // 遍历一个哈希槽（v是传进来的 光标值）
        de = t0->table[v & m0];
        while (de) {
            next = de->next;
            fn(privdata, de);
            de = next;
        }

    } else {                    // rehash
        t0 = &d->ht[0];
        t1 = &d->ht[1];

        /* Make sure t0 is the smaller and t1 is the bigger table */
        if (t0->size > t1->size) {
            t0 = &d->ht[1];
            t1 = &d->ht[0];
        }

        m0 = t0->sizemask;
        m1 = t1->sizemask;

        /* Emit entries at cursor */
        if (bucketfn) bucketfn(privdata, &t0->table[v & m0]);
        de = t0->table[v & m0];
        while (de) {
            next = de->next;
            fn(privdata, de);
            de = next;
        }

        /* Iterate over indices in larger table that are the expansion
         * of the index pointed to by the cursor in the smaller table */
        do {
            /* Emit entries at cursor */
            if (bucketfn) bucketfn(privdata, &t1->table[v & m1]);
            de = t1->table[v & m1];
            while (de) {
                next = de->next;
                fn(privdata, de);
                de = next;
            }

            /* Increment bits not covered by the smaller mask */
            v = (((v | m0) + 1) & ~m0) | (v & m0);

            /* Continue while bits covered by mask difference is non-zero */
        } while (v & (m0 ^ m1));
    }

    // 将游标v 的unmasked比特位都置为1
    // ~ 按位取反
    // & 按位与
    // | 按位或
    v |= ~m0;

    v = rev(v); // 反转v
    v++;        // v+1
    v = rev(v); // 再次反转v，即得到下一个游标值

    return v;   // 返回增加后的光标值
}

/* ------------------------- private functions ------------------------------ */

//// 判断是否需要扩容
static int _dictExpandIfNeeded(dict *d)
{
    if (dictIsRehashing(d)) return DICT_OK;                             // 正在rehash不扩容

    if (d->ht[0].size == 0) return dictExpand(d, DICT_HT_INITIAL_SIZE); // 哈希表为空，初始化

    //used/size >= 1时候，可能会rehash（此时还能忍受，如果有fork进程暂时不rehash，此时涉及到操作系统的写时复制问题）
    //used/size >= 5时候，强制必须rehash
    if (d->ht[0].used >= d->ht[0].size &&
        (dict_can_resize ||
         d->ht[0].used/d->ht[0].size > dict_force_resize_ratio))
    {
        return dictExpand(d, d->ht[0].used*2);
    }
    return DICT_OK;
}

//// 计算hash表数组大小，最小容量为4，每次乘2，直到算出>size的容量停止
static unsigned long _dictNextPower(unsigned long size)
{
    unsigned long i = DICT_HT_INITIAL_SIZE;

    if (size >= LONG_MAX) return LONG_MAX;
    while(1) {
        if (i >= size)
            return i;
        i *= 2;
    }
}

//// 根据key算出index（哈希数组的索引），如果存在返回-1，并设置existing为存在的节点
static int _dictKeyIndex(dict *d, const void *key, unsigned int hash, dictEntry **existing)
{
    unsigned int idx, table;
    dictEntry *he;
    if (existing) *existing = NULL;

    // 判断是否需要扩容
    if (_dictExpandIfNeeded(d) == DICT_ERR)
        return -1;


    for (table = 0; table <= 1; table++) {
        // idx就是根据hash值算出的哈希槽，he为链表（解决hash冲突的链表）
        idx = hash & d->ht[table].sizemask;
        he = d->ht[table].table[idx];

        // 在链表中遍历，比较key
        while(he) {
            if (key==he->key || dictCompareKeys(d, key, he->key)) {// 如果找到返回-1
                if (existing) *existing = he;
                return -1;
            }
            he = he->next;
        }
        if (!dictIsRehashing(d)) break;
    }
    return idx;
}

//// 清空字典
void dictEmpty(dict *d, void(callback)(void*)) {
    _dictClear(d,&d->ht[0],callback);
    _dictClear(d,&d->ht[1],callback);
    d->rehashidx = -1;
    d->iterators = 0;
}

//// 设置可以resize
void dictEnableResize(void) {
    dict_can_resize = 1;
}

//// 禁止resize
void dictDisableResize(void) {
    dict_can_resize = 0;
}

//// 根据key计算hash值
unsigned int dictGetHash(dict *d, const void *key) {
    return dictHashKey(d, key);
}

dictEntry **dictFindEntryRefByPtrAndHash(dict *d, const void *oldptr, unsigned int hash) {
    dictEntry *he, **heref;
    unsigned int idx, table;

    if (d->ht[0].used + d->ht[1].used == 0) return NULL;    // 字典为空，直接返回
    for (table = 0; table <= 1; table++) {
        idx = hash & d->ht[table].sizemask;                 // 根据hash值计算哈希槽
        heref = &d->ht[table].table[idx];
        he = *heref;

        // 遍历冲突链表
        while(he) {
            if (oldptr==he->key)
                return heref;
            heref = &he->next;
            he = *heref;
        }
        if (!dictIsRehashing(d)) return NULL;
    }
    return NULL;
}

/* ------------------------------- Debugging ---------------------------------*/

#define DICT_STATS_VECTLEN 50
size_t _dictGetStatsHt(char *buf, size_t bufsize, dictht *ht, int tableid) {
    unsigned long i, slots = 0, chainlen, maxchainlen = 0;
    unsigned long totchainlen = 0;
    unsigned long clvector[DICT_STATS_VECTLEN];                 // 用来记录哈希槽的元素个数分布情况eg: clvector[0]=10 0个元素的哈希槽有10个
    size_t l = 0;

    if (ht->used == 0) {                                        // 字典为空，直接返回
        return snprintf(buf,bufsize,
            "No stats available for empty dictionaries\n");
    }

    for (i = 0; i < DICT_STATS_VECTLEN; i++) clvector[i] = 0;   // 状态数组初始化

    // 根据哈希槽个数（数组大小）遍历
    for (i = 0; i < ht->size; i++) {
        dictEntry *he;

        if (ht->table[i] == NULL) {                             // 哈希槽为空，clvector[0]++记录
            clvector[0]++;
            continue;
        }
        slots++;                                                // 记录哈希槽的个数

        // 遍历哈希槽里的冲突链表
        chainlen = 0;                                           // 哈希槽中的元素个数
        he = ht->table[i];
        while(he) {
            chainlen++;
            he = he->next;
        }

        // 根据元素个数(超过50的统一按照49算) clvector[元素个数]++
        // 统计哈希槽中元素个数的分布情况
        clvector[(chainlen < DICT_STATS_VECTLEN) ? chainlen : (DICT_STATS_VECTLEN-1)]++;


        if (chainlen > maxchainlen) maxchainlen = chainlen;
        totchainlen += chainlen;                                // 记录所有的元素个数
    }

    // 格式化输出
    l += snprintf(buf+l,bufsize-l,
        "Hash table %d stats (%s):\n"
        " table size: %ld\n"
        " number of elements: %ld\n"
        " different slots: %ld\n"
        " max chain length: %ld\n"
        " avg chain length (counted): %.02f\n"
        " avg chain length (computed): %.02f\n"
        " Chain length distribution:\n",
        tableid, (tableid == 0) ? "main hash table" : "rehashing target",
        ht->size,
        ht->used,
        slots,
        maxchainlen,
        (float)totchainlen/slots,
        (float)ht->used/slots);

    // 格式化clvector数组记录的信息，展示哈希槽的元素个数分布情况
    for (i = 0; i < DICT_STATS_VECTLEN-1; i++) {
        if (clvector[i] == 0) continue;
        if (l >= bufsize) break;
        l += snprintf(buf+l,bufsize-l,
            "   %s%ld: %ld (%.02f%%)\n",
            (i == DICT_STATS_VECTLEN-1)?">= ":"",
            i, clvector[i], ((float)clvector[i]/ht->size)*100);
    }

    /* Unlike snprintf(), teturn the number of characters actually written. */
    if (bufsize) buf[bufsize-1] = '\0';
    return strlen(buf);
}

// 获取dict状态
void dictGetStats(char *buf, size_t bufsize, dict *d) {
    size_t l;
    char *orig_buf = buf;
    size_t orig_bufsize = bufsize;

    l = _dictGetStatsHt(buf,bufsize,&d->ht[0],0);
    buf += l;
    bufsize -= l;
    if (dictIsRehashing(d) && bufsize > 0) {
        _dictGetStatsHt(buf,bufsize,&d->ht[1],1);
    }
    /* Make sure there is a NULL term at the end. */
    if (orig_bufsize) orig_buf[orig_bufsize-1] = '\0';
}

/* ------------------------------- Benchmark ---------------------------------*/

#ifdef DICT_BENCHMARK_MAIN

#include "sds.h"

uint64_t hashCallback(const void *key) {
    return dictGenHashFunction((unsigned char*)key, sdslen((char*)key));
}

int compareCallback(void *privdata, const void *key1, const void *key2) {
    int l1,l2;
    DICT_NOTUSED(privdata);

    l1 = sdslen((sds)key1);
    l2 = sdslen((sds)key2);
    if (l1 != l2) return 0;
    return memcmp(key1, key2, l1) == 0;
}

void freeCallback(void *privdata, void *val) {
    DICT_NOTUSED(privdata);

    sdsfree(val);
}

dictType BenchmarkDictType = {
    hashCallback,
    NULL,
    NULL,
    compareCallback,
    freeCallback,
    NULL
};

#define start_benchmark() start = timeInMilliseconds()
#define end_benchmark(msg) do { \
    elapsed = timeInMilliseconds()-start; \
    printf(msg ": %ld items in %lld ms\n", count, elapsed); \
} while(0);

/* dict-benchmark [count] */
int main(int argc, char **argv) {
    long j;
    long long start, elapsed;
    dict *dict = dictCreate(&BenchmarkDictType,NULL);
    long count = 0;

    if (argc == 2) {
        count = strtol(argv[1],NULL,10);
    } else {
        count = 5000000;
    }

    start_benchmark();
    for (j = 0; j < count; j++) {
        int retval = dictAdd(dict,sdsfromlonglong(j),(void*)j);
        assert(retval == DICT_OK);
    }
    end_benchmark("Inserting");
    assert((long)dictSize(dict) == count);

    /* Wait for rehashing. */
    while (dictIsRehashing(dict)) {
        dictRehashMilliseconds(dict,100);
    }

    start_benchmark();
    for (j = 0; j < count; j++) {
        sds key = sdsfromlonglong(j);
        dictEntry *de = dictFind(dict,key);
        assert(de != NULL);
        sdsfree(key);
    }
    end_benchmark("Linear access of existing elements");

    start_benchmark();
    for (j = 0; j < count; j++) {
        sds key = sdsfromlonglong(j);
        dictEntry *de = dictFind(dict,key);
        assert(de != NULL);
        sdsfree(key);
    }
    end_benchmark("Linear access of existing elements (2nd round)");

    start_benchmark();
    for (j = 0; j < count; j++) {
        sds key = sdsfromlonglong(rand() % count);
        dictEntry *de = dictFind(dict,key);
        assert(de != NULL);
        sdsfree(key);
    }
    end_benchmark("Random access of existing elements");

    start_benchmark();
    for (j = 0; j < count; j++) {
        sds key = sdsfromlonglong(rand() % count);
        key[0] = 'X';
        dictEntry *de = dictFind(dict,key);
        assert(de == NULL);
        sdsfree(key);
    }
    end_benchmark("Accessing missing");

    start_benchmark();
    for (j = 0; j < count; j++) {
        sds key = sdsfromlonglong(j);
        int retval = dictDelete(dict,key);
        assert(retval == DICT_OK);
        key[0] += 17; /* Change first number to letter. */
        retval = dictAdd(dict,key,(void*)j);
        assert(retval == DICT_OK);
    }
    end_benchmark("Removing and adding");
}
#endif
