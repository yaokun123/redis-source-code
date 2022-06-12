#ifndef __INTSET_H
#define __INTSET_H
#include <stdint.h>

//// 如果一个集合(set)满足只保存整数元素和元素数量不多这两个条件，那么Redis就会采用intset来保存这个数据集
//// intset可以保存类型为int16_t，int32_t，int64_t的整数值，并且保证集合中不会出现重复元素。
//// 注意，编码在保存时是按照小端的方式保存的，也就是说在大端系统中，还需要将实际的编码值翻转之后，才能存储到encoding中，比如：is->encoding =intrev32ifbe(INTSET_ENC_INT16); 其中intrev32ifbe表示将32位整数按照字节进行翻转。
typedef struct intset {
    uint32_t encoding;      // 整数的编码模式，共有三种编码，分别是：INTSET_ENC_INT16、INTSET_ENC_INT32和INTSET_ENC_INT64
    uint32_t length;        // 保存的整数个数
    // 柔性数组成员，它是整数集合的底层实现，整数集合中的每个元素都是contents数组的一个数组项，各个项在数组中按值从小到大有序地排列，并且数组中不包含任何重复项。
    // 虽然contents的类型是int8_t，但它可以保存类型为int16_t，int32_t，int64_t的整数值。可以将contents想象为一段连续的内存空间，该空间的实际大小是encoding*length。
    int8_t contents[];
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
