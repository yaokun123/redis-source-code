#ifndef __QUICKLIST_H__
#define __QUICKLIST_H__

/* Node, quicklist, and Iterator are the only data structures used currently. */

//// quicklist节点的数据结构（每个quicklistnode也占用32个字节）
// 8 + 8 + 8 + 4 + 4
typedef struct quicklistNode {
    struct quicklistNode *prev;     // 指向上一个ziplist节点
    struct quicklistNode *next;     // 指向下一个ziplist节点
    unsigned char *zl;              // 数据指针，如果没有被压缩，就指向ziplist结构，反之指向quicklistLZF结构
    unsigned int sz;                // 表示指向ziplist结构的总长度(内存占用长度)
    unsigned int count : 16;        // 表示ziplist中的数据项个数
    unsigned int encoding : 2;      // 编码方式，1--ziplist，2--quicklistLZF
    unsigned int container : 2;     // 预留字段，存放数据的方式，1--NONE，2--ziplist
    unsigned int recompress : 1;    // 解压标记，当查看一个被压缩的数据时，需要暂时解压，标记此参数为1，之后再重新进行压缩
    unsigned int attempted_compress : 1; // 测试相关
    unsigned int extra : 10;        // 扩展字段，暂时没用
} quicklistNode;

// 压缩数据结构
typedef struct quicklistLZF {
    unsigned int sz;                // LZF压缩后占用的字节数
    char compressed[];              // 柔性数组，指向数据部分
} quicklistLZF;

//// quicklist的数据结构定义(每个quicklist结构占用32个字节的空间)
// 8 + 8 + 8 + 4 + 4 = 32
typedef struct quicklist {
    quicklistNode *head;            // 指向quicklist的头部
    quicklistNode *tail;            // 指向quicklist的尾部
    unsigned long count;            // 列表中所有数据项的个数总和
    unsigned int len;               // quicklist节点的个数，即ziplist的个数
    int fill : 16;                  // ziplist大小限定，由list-max-ziplist-size给定
    unsigned int compress : 16;     // 节点压缩深度设置，由list-compress-depth给定
} quicklist;

// quicklist的迭代器结构
typedef struct quicklistIter {
    const quicklist *quicklist;     // 指向所在quicklist的指针
    quicklistNode *current;         // 指向当前节点的指针
    unsigned char *zi;              // 指向当前节点的ziplist
    long offset;                    // 当前ziplist中的偏移地址
    int direction;                  // 迭代器的方向
} quicklistIter;

// 表示quicklist节点中ziplist里的一个节点结构
typedef struct quicklistEntry {
    const quicklist *quicklist;     // 指向所在quicklist的指针
    quicklistNode *node;            // 指向当前节点的指针
    unsigned char *zi;              // 指向当前节点的ziplist
    unsigned char *value;           // 当前指向的ziplist中的节点的字符串值
    long long longval;              // 当前指向的ziplist中的节点的整型值
    unsigned int sz;                // 当前指向的ziplist中的节点的字节大小
    int offset;                     // 当前指向的ziplist中的节点相对于ziplist的偏移量
} quicklistEntry;

#define QUICKLIST_HEAD 0
#define QUICKLIST_TAIL -1

/* quicklist node encodings */
#define QUICKLIST_NODE_ENCODING_RAW 1
#define QUICKLIST_NODE_ENCODING_LZF 2

/* quicklist compression disable */
#define QUICKLIST_NOCOMPRESS 0

/* quicklist container formats */
#define QUICKLIST_NODE_CONTAINER_NONE 1
#define QUICKLIST_NODE_CONTAINER_ZIPLIST 2

#define quicklistNodeIsCompressed(node)                                        \
    ((node)->encoding == QUICKLIST_NODE_ENCODING_LZF)

/* Prototypes */
quicklist *quicklistCreate(void);
quicklist *quicklistNew(int fill, int compress);
void quicklistSetCompressDepth(quicklist *quicklist, int depth);
void quicklistSetFill(quicklist *quicklist, int fill);
void quicklistSetOptions(quicklist *quicklist, int fill, int depth);
void quicklistRelease(quicklist *quicklist);
int quicklistPushHead(quicklist *quicklist, void *value, const size_t sz);
int quicklistPushTail(quicklist *quicklist, void *value, const size_t sz);
void quicklistPush(quicklist *quicklist, void *value, const size_t sz,
                   int where);
void quicklistAppendZiplist(quicklist *quicklist, unsigned char *zl);
quicklist *quicklistAppendValuesFromZiplist(quicklist *quicklist,
                                            unsigned char *zl);
quicklist *quicklistCreateFromZiplist(int fill, int compress,
                                      unsigned char *zl);
void quicklistInsertAfter(quicklist *quicklist, quicklistEntry *node,
                          void *value, const size_t sz);
void quicklistInsertBefore(quicklist *quicklist, quicklistEntry *node,
                           void *value, const size_t sz);
void quicklistDelEntry(quicklistIter *iter, quicklistEntry *entry);
int quicklistReplaceAtIndex(quicklist *quicklist, long index, void *data,
                            int sz);
int quicklistDelRange(quicklist *quicklist, const long start, const long stop);
quicklistIter *quicklistGetIterator(const quicklist *quicklist, int direction);
quicklistIter *quicklistGetIteratorAtIdx(const quicklist *quicklist,
                                         int direction, const long long idx);
int quicklistNext(quicklistIter *iter, quicklistEntry *node);
void quicklistReleaseIterator(quicklistIter *iter);
quicklist *quicklistDup(quicklist *orig);
int quicklistIndex(const quicklist *quicklist, const long long index,
                   quicklistEntry *entry);
void quicklistRewind(quicklist *quicklist, quicklistIter *li);
void quicklistRewindTail(quicklist *quicklist, quicklistIter *li);
void quicklistRotate(quicklist *quicklist);
int quicklistPopCustom(quicklist *quicklist, int where, unsigned char **data,
                       unsigned int *sz, long long *sval,
                       void *(*saver)(unsigned char *data, unsigned int sz));
int quicklistPop(quicklist *quicklist, int where, unsigned char **data,
                 unsigned int *sz, long long *slong);
unsigned int quicklistCount(const quicklist *ql);
int quicklistCompare(unsigned char *p1, unsigned char *p2, int p2_len);
size_t quicklistGetLzf(const quicklistNode *node, void **data);

#ifdef REDIS_TEST
int quicklistTest(int argc, char *argv[]);
#endif

/* Directions for iterators */
#define AL_START_HEAD 0
#define AL_START_TAIL 1

#endif /* __QUICKLIST_H__ */
