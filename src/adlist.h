#ifndef __ADLIST_H__
#define __ADLIST_H__


//// 链表节点定义
typedef struct listNode {
    struct listNode *prev;          // 指向前一个节点
    struct listNode *next;          // 指向后一个节点
    void *value;                    // 节点值
} listNode;


//// 链表定义
typedef struct list {
    listNode *head;                 // 指向链表头节点
    listNode *tail;                 // 指向链表尾节点
    void *(*dup)(void *ptr);        // 自定义节点值复制函数
    void (*free)(void *ptr);        // 自定义节点值释放函数
    int (*match)(void *ptr, void *key);// 自定义节点值匹配函数
    unsigned long len;              // 链表长度
} list;


//// 迭代器结构，能正序和逆序的访问list结构
typedef struct listIter {
    listNode *next;                 // 指向下一个节点
    int direction;                  // 方向参数，正序和逆序
} listIter;


/* Functions implemented as macros */
#define listLength(l) ((l)->len)                    // 获取list长度
#define listFirst(l) ((l)->head)                    // 获取list头节点指针
#define listLast(l) ((l)->tail)                     // 获取list尾节点指针
#define listPrevNode(n) ((n)->prev)                 // 获取当前节点前一个节点
#define listNextNode(n) ((n)->next)                 // 获取当前节点后一个节点
#define listNodeValue(n) ((n)->value)               // 获取当前节点的值

#define listSetDupMethod(l,m) ((l)->dup = (m))      // 设定节点值复制函数
#define listSetFreeMethod(l,m) ((l)->free = (m))    // 设定节点值释放函数
#define listSetMatchMethod(l,m) ((l)->match = (m))  // 设定节点值匹配函数

#define listGetDupMethod(l) ((l)->dup)              // 获取节点值赋值函数
#define listGetFree(l) ((l)->free)                  // 获取节点值释放函数
#define listGetMatchMethod(l) ((l)->match)          // 获取节点值匹配函数

/* Prototypes */
list *listCreate(void);                                                             // sdlist创建，创建一个空的链表
void listRelease(list *list);                                                       // sdlist释放，释放整个链表
void listEmpty(list *list);                                                         // 置空list
list *listAddNodeHead(list *list, void *value);                                     // 该函数向list的头部插入一个节点
list *listAddNodeTail(list *list, void *value);                                     // 向尾部添加节点
list *listInsertNode(list *list, listNode *old_node, void *value, int after);       // 向任意位置插入节点
void listDelNode(list *list, listNode *node);                                       // 删除节点
listIter *listGetIterator(list *list, int direction);                               // 获取迭代器
listNode *listNext(listIter *iter);                                                 // 获取迭代器下一个节点
void listReleaseIterator(listIter *iter);                                           // 释放迭代器
list *listDup(list *orig);                                                          // 链表复制
listNode *listSearchKey(list *list, void *key);                                     // 根据给定节点值，在链表中查找该节点
listNode *listIndex(list *list, long index);                                        // 根据序号来查找节点
void listRewind(list *list, listIter *li);                                          // 重置正向迭代器
void listRewindTail(list *list, listIter *li);                                      // 重置逆向迭代器
void listRotate(list *list);                                                        // 旋转函数
void listJoin(list *l, list *o);                                                    // 合并链表，将合并到l上，释放o

//// 定义迭代器的顺序
#define AL_START_HEAD 0                         // 从头开始
#define AL_START_TAIL 1                         // 从尾开始

#endif /* __ADLIST_H__ */
