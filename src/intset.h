#ifndef __INTSET_H
#define __INTSET_H
#include <stdint.h>

//如果一个集合满足只保存整数元素和元素数量不多这两个条件，那么Redis就会采用intset来保存这个数据集
typedef struct intset {
    uint32_t encoding;      // 编码模式
    uint32_t length;        // 长度
    int8_t contents[];      // 数据部分
} intset;

intset *intsetNew(void);
intset *intsetAdd(intset *is, int64_t value, uint8_t *success);
intset *intsetRemove(intset *is, int64_t value, int *success);
uint8_t intsetFind(intset *is, int64_t value);
int64_t intsetRandom(intset *is);
uint8_t intsetGet(intset *is, uint32_t pos, int64_t *value);
uint32_t intsetLen(const intset *is);
size_t intsetBlobLen(intset *is);

#ifdef REDIS_TEST
int intsetTest(int argc, char *argv[]);
#endif

#endif // __INTSET_H
