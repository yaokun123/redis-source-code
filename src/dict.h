#include <stdint.h>

#ifndef __DICT_H
#define __DICT_H

#define DICT_OK 0
#define DICT_ERR 1

/* Unused arguments generate annoying warnings... */
#define DICT_NOTUSED(V) ((void) V)

//// 哈希表节点
typedef struct dictEntry {
    void *key;                  // 键
    union {
        void *val;
        uint64_t u64;
        int64_t s64;
        double d;
    } v;                        // 值（除了数值部分，其他都是指针）
    struct dictEntry *next;     // 指向下一个哈希表节点，用来解决hash冲突的。此处可以看出字典采用了链地址法解决哈希冲突
} dictEntry;


//// 字典类型函数
typedef struct dictType {
    uint64_t (*hashFunction)(const void *key);                                  // 计算哈希值的函数
    void *(*keyDup)(void *privdata, const void *key);                           // 复制键的函数
    void *(*valDup)(void *privdata, const void *obj);                           // 复制值的函数
    int (*keyCompare)(void *privdata, const void *key1, const void *key2);      // 比较键的函数
    void (*keyDestructor)(void *privdata, void *key);                           // 销毁键的函数
    void (*valDestructor)(void *privdata, void *obj);                           // 销毁值的函数
} dictType;


//// 哈希表
typedef struct dictht {
    dictEntry **table;          // 哈希表数组
    unsigned long size;         // 哈希表大小，也即是table数组的大小
    unsigned long sizemask;     // 哈希表大小掩码，用于计算索引值。总是等于size-1
    unsigned long used;         // 该哈希表中已有节点的数量
} dictht;


//// 字典
typedef struct dict {
    dictType *type;             // 字典类型，保存一些用于操作特定类型键值对的函数
    void *privdata;             // 私有数据，保存需要传给那些类型特定函数的可选数据
    dictht ht[2];               // 一个字典结构包括两个哈希表。一般情况下，字典只使用ht[0]哈希表，渐进式rehash时使用ht[1]。
    long rehashidx;             // rehash索引，记录了rehash目前的进度。不进行rehash时其值为-1。
    unsigned long iterators;    // 当前正在使用的迭代器数量
} dict;


//// 字典迭代器
/***
 * 如果 safe 属性的值为 1 ，那么在迭代进行的过程中，程序仍然可以执行 dictAdd 、 dictFind 和其他函数，对字典进行修改。
 * 如果 safe 不为 1 ，那么程序只会调用 dictNext 对字典进行迭代，而不对字典进行修改。
 */
typedef struct dictIterator {
    dict *d;                            // 被迭代的字典
    long index;                         // index ：迭代器当前所指向的哈希表索引位置。
    int table, safe;                    // table ：正在被迭代的哈希表号码，值可以是 0 或 1 。
                                        // safe ：标识这个迭代器是否安全

    // entry ：当前迭代到的节点的指针
    // nextEntry ：当前迭代节点的下一个节点
    //             因为在安全迭代器运作时， entry 所指向的节点可能会被修改，
    //             所以需要一个额外的指针来保存下一节点的位置，
    //             从而防止指针丢失
    dictEntry *entry, *nextEntry;

    // 指纹，指纹是代表字典在给定时间下的状态的一个64位的数，它通过字典的几个属性的异或可以得到。
    // 当我们初始化一个不安全的迭代器时，就会得到字典的指纹，当迭代器释放时，我们会再次检查指纹。
    // 如果两次的指纹不一致，那么意味着不安全迭代器的调用者在迭代过程中对字典进行了非法的调用，报错。
    long long fingerprint;
} dictIterator;

typedef void (dictScanFunction)(void *privdata, const dictEntry *de);
typedef void (dictScanBucketFunction)(void *privdata, dictEntry **bucketref);

/* This is the initial size of every hash table */
#define DICT_HT_INITIAL_SIZE     4

/* ------------------------------- Macros ------------------------------------*/
#define dictFreeVal(d, entry) \
    if ((d)->type->valDestructor) \
        (d)->type->valDestructor((d)->privdata, (entry)->v.val)

#define dictSetVal(d, entry, _val_) do { \
    if ((d)->type->valDup) \
        (entry)->v.val = (d)->type->valDup((d)->privdata, _val_); \
    else \
        (entry)->v.val = (_val_); \
} while(0)

#define dictSetSignedIntegerVal(entry, _val_) \
    do { (entry)->v.s64 = _val_; } while(0)

#define dictSetUnsignedIntegerVal(entry, _val_) \
    do { (entry)->v.u64 = _val_; } while(0)

#define dictSetDoubleVal(entry, _val_) \
    do { (entry)->v.d = _val_; } while(0)

#define dictFreeKey(d, entry) \
    if ((d)->type->keyDestructor) \
        (d)->type->keyDestructor((d)->privdata, (entry)->key)

#define dictSetKey(d, entry, _key_) do { \
    if ((d)->type->keyDup) \
        (entry)->key = (d)->type->keyDup((d)->privdata, _key_); \
    else \
        (entry)->key = (_key_); \
} while(0)

#define dictCompareKeys(d, key1, key2) \
    (((d)->type->keyCompare) ? \
        (d)->type->keyCompare((d)->privdata, key1, key2) : \
        (key1) == (key2))

#define dictHashKey(d, key) (d)->type->hashFunction(key)
#define dictGetKey(he) ((he)->key)
#define dictGetVal(he) ((he)->v.val)
#define dictGetSignedIntegerVal(he) ((he)->v.s64)
#define dictGetUnsignedIntegerVal(he) ((he)->v.u64)
#define dictGetDoubleVal(he) ((he)->v.d)
#define dictSlots(d) ((d)->ht[0].size+(d)->ht[1].size)
#define dictSize(d) ((d)->ht[0].used+(d)->ht[1].used)
#define dictIsRehashing(d) ((d)->rehashidx != -1)

/* API */
dict *dictCreate(dictType *type, void *privDataPtr);
int dictExpand(dict *d, unsigned long size);
int dictAdd(dict *d, void *key, void *val);
dictEntry *dictAddRaw(dict *d, void *key, dictEntry **existing);
dictEntry *dictAddOrFind(dict *d, void *key);
int dictReplace(dict *d, void *key, void *val);
int dictDelete(dict *d, const void *key);
dictEntry *dictUnlink(dict *ht, const void *key);
void dictFreeUnlinkedEntry(dict *d, dictEntry *he);
void dictRelease(dict *d);
dictEntry * dictFind(dict *d, const void *key);
void *dictFetchValue(dict *d, const void *key);
int dictResize(dict *d);
dictIterator *dictGetIterator(dict *d);
dictIterator *dictGetSafeIterator(dict *d);
dictEntry *dictNext(dictIterator *iter);
void dictReleaseIterator(dictIterator *iter);
dictEntry *dictGetRandomKey(dict *d);
unsigned int dictGetSomeKeys(dict *d, dictEntry **des, unsigned int count);
void dictGetStats(char *buf, size_t bufsize, dict *d);
uint64_t dictGenHashFunction(const void *key, int len);
uint64_t dictGenCaseHashFunction(const unsigned char *buf, int len);
void dictEmpty(dict *d, void(callback)(void*));
void dictEnableResize(void);
void dictDisableResize(void);
int dictRehash(dict *d, int n);
int dictRehashMilliseconds(dict *d, int ms);
void dictSetHashFunctionSeed(uint8_t *seed);
uint8_t *dictGetHashFunctionSeed(void);
unsigned long dictScan(dict *d, unsigned long v, dictScanFunction *fn, dictScanBucketFunction *bucketfn, void *privdata);
unsigned int dictGetHash(dict *d, const void *key);
dictEntry **dictFindEntryRefByPtrAndHash(dict *d, const void *oldptr, unsigned int hash);

/* Hash table types */
extern dictType dictTypeHeapStringCopyKey;
extern dictType dictTypeHeapStrings;
extern dictType dictTypeHeapStringCopyKeyValue;

#endif /* __DICT_H */
