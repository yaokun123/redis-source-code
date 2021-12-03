#ifndef __ZMALLOC_H
#define __ZMALLOC_H

/* Double expansion needed for stringification of macro values. */
#define __xstr(s) __str(s)
#define __str(s) #s

#if defined(USE_TCMALLOC)
#define ZMALLOC_LIB ("tcmalloc-" __xstr(TC_VERSION_MAJOR) "." __xstr(TC_VERSION_MINOR))
#include <google/tcmalloc.h>
#if (TC_VERSION_MAJOR == 1 && TC_VERSION_MINOR >= 6) || (TC_VERSION_MAJOR > 1)
#define HAVE_MALLOC_SIZE 1
#define zmalloc_size(p) tc_malloc_size(p)
#else
#error "Newer version of tcmalloc required"
#endif

#elif defined(USE_JEMALLOC)
#define ZMALLOC_LIB ("jemalloc-" __xstr(JEMALLOC_VERSION_MAJOR) "." __xstr(JEMALLOC_VERSION_MINOR) "." __xstr(JEMALLOC_VERSION_BUGFIX))
#include <jemalloc/jemalloc.h>
#if (JEMALLOC_VERSION_MAJOR == 2 && JEMALLOC_VERSION_MINOR >= 1) || (JEMALLOC_VERSION_MAJOR > 2)
#define HAVE_MALLOC_SIZE 1
#define zmalloc_size(p) je_malloc_usable_size(p)
#else
#error "Newer version of jemalloc required"
#endif

#elif defined(__APPLE__)
#include <malloc/malloc.h>
#define HAVE_MALLOC_SIZE 1
#define zmalloc_size(p) malloc_size(p)
#endif

#ifndef ZMALLOC_LIB
#define ZMALLOC_LIB "libc"
#endif

/* We can enable the Redis defrag capabilities only if we are using Jemalloc
 * and the version used is our special version modified for Redis having
 * the ability to return per-allocation fragmentation hints. */
#if defined(USE_JEMALLOC) && defined(JEMALLOC_FRAG_HINT)
#define HAVE_DEFRAG
#endif

void *zmalloc(size_t size);                                 // 调用zmalloc函数，申请size大小的空间
void *zcalloc(size_t size);                                 // 调用系统函数calloc申请内存空间
void *zrealloc(void *ptr, size_t size);                     // 原内存重新调整为size空间的大小
void zfree(void *ptr);                                      // 调用zfree释放内存空间


char *zstrdup(const char *s);                               // 字符串复制方法
size_t zmalloc_used_memory(void);                           // 获取当前占用的内存空间大小
void zmalloc_set_oom_handler(void (*oom_handler)(size_t));  // 可自定义设置内存
float zmalloc_get_fragmentation_ratio(size_t rss);          // 获取所给内存和已使用内存的大小之比
size_t zmalloc_get_rss(void);                               // 获取RSS信息（Resident Set Size）
size_t zmalloc_get_private_dirty(long pid);                 // 获取实际内存大小
size_t zmalloc_get_smap_bytes_by_field(char *field, long pid);// 获取/proc/self/smaps字段的字节数
size_t zmalloc_get_memory_size(void);                       // 获取物理内存大小
void zlibc_free(void *ptr);                                 // 原始系统free释放方法

#ifdef HAVE_DEFRAG
void zfree_no_tcache(void *ptr);
void *zmalloc_no_tcache(size_t size);
#endif

#ifndef HAVE_MALLOC_SIZE
size_t zmalloc_size(void *ptr);
#endif

#endif /* __ZMALLOC_H */
