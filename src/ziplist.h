#ifndef _ZIPLIST_H
#define _ZIPLIST_H

#define ZIPLIST_HEAD 0
#define ZIPLIST_TAIL 1

/**
 *     4        4      2                                 1
 * <zlbytes><zltail><zllen><entry1><entry2>...<entryN><zlend>
 *
 * zlbytes是一个uint32_t类型的整数，表示整个ziplist占用的内存字节数
 * zltail是一个uint32_t类型的整数，表示ziplist中最后一个entry的偏移量，通过该偏移量，无需遍历整个ziplist就可以确定尾结点的地址
 * zllen是一个uint16_t类型的整数，表示ziplist中的entry数目。如果该值小于UINT16_MAX(65535)，则该属性值就是ziplist的entry数目，若该值等于UINT16_MAX(65535)，则还需要遍历整个ziplist才能得到真正的entry数目
 * entryX表示ziplist的结点，每个entry的长度不定
 * lend是一个uint8_t类型的整数，其值为0xFF(255)，表示ziplist的结尾。
 */

unsigned char *ziplistNew(void);
unsigned char *ziplistMerge(unsigned char **first, unsigned char **second);
unsigned char *ziplistPush(unsigned char *zl, unsigned char *s, unsigned int slen, int where);
unsigned char *ziplistIndex(unsigned char *zl, int index);
unsigned char *ziplistNext(unsigned char *zl, unsigned char *p);
unsigned char *ziplistPrev(unsigned char *zl, unsigned char *p);
unsigned int ziplistGet(unsigned char *p, unsigned char **sval, unsigned int *slen, long long *lval);
unsigned char *ziplistInsert(unsigned char *zl, unsigned char *p, unsigned char *s, unsigned int slen);
unsigned char *ziplistDelete(unsigned char *zl, unsigned char **p);
unsigned char *ziplistDeleteRange(unsigned char *zl, int index, unsigned int num);
unsigned int ziplistCompare(unsigned char *p, unsigned char *s, unsigned int slen);
unsigned char *ziplistFind(unsigned char *p, unsigned char *vstr, unsigned int vlen, unsigned int skip);
unsigned int ziplistLen(unsigned char *zl);
size_t ziplistBlobLen(unsigned char *zl);
void ziplistRepr(unsigned char *zl);

#ifdef REDIS_TEST
int ziplistTest(int argc, char *argv[]);
#endif

#endif /* _ZIPLIST_H */
