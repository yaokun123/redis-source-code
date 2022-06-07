#include <stdlib.h>
#include "adlist.h"
#include "zmalloc.h"

//// 创建一个空的链表，此时只是有了个list的结构，还没有任何节点
list *listCreate(void)
{
    struct list *list;                                  // 定义一个链表指针

    if ((list = zmalloc(sizeof(*list))) == NULL)        // 申请内存一个struct list的大小
        return NULL;
    list->head = list->tail = NULL;                     // 空链表的头指针和尾指针均为空
    list->len = 0;                                      // 设定长度
    list->dup = NULL;                                   // 自定义复制函数初始化
    list->free = NULL;                                  // 自定义释放函数初始化
    list->match = NULL;                                 // 自定义匹配函数初始化
    return list;
}

//// 清空一个链表
void listEmpty(list *list)
{
    unsigned long len;
    listNode *current, *next;

    current = list->head;                               // 头部节点
    len = list->len;                                    // 链表长度

    //// 根据链表长度循环释放每一个节点
    while(len--) {
        next = current->next;
        if (list->free) list->free(current->value);     // 如果有自定义释放函数调用之
        zfree(current);
        current = next;
    }
    list->head = list->tail = NULL;                     // 设置链表头部节点和尾部节点都为空
    list->len = 0;                                      // 链表长度为0
}

//// 释放整个链表
void listRelease(list *list)
{
    listEmpty(list);    // 清空链表
    zfree(list);        // 释放链表头
}

//// 向list的头部插入一个节点
list *listAddNodeHead(list *list, void *value)
{
    listNode *node;

    if ((node = zmalloc(sizeof(*node))) == NULL)        // 分配node节点空间
        return NULL;
    node->value = value;                                // 设置node的值
    if (list->len == 0) {                               // 如果链表为空
        list->head = list->tail = node;
        node->prev = node->next = NULL;
    } else {                                            // 链表不为空
        node->prev = NULL;
        node->next = list->head;
        list->head->prev = node;
        list->head = node;
    }
    list->len++;                                        // 链表长度++
    return list;
}

//// 向尾部添加节点
list *listAddNodeTail(list *list, void *value)
{
    listNode *node;

    if ((node = zmalloc(sizeof(*node))) == NULL)
        return NULL;
    node->value = value;
    if (list->len == 0) {
        list->head = list->tail = node;
        node->prev = node->next = NULL;
    } else {
        node->prev = list->tail;
        node->next = NULL;
        list->tail->next = node;
        list->tail = node;
    }
    list->len++;
    return list;
}

//// 向任意位置插入节点
list *listInsertNode(list *list, listNode *old_node, void *value, int after) {
    listNode *node;

    if ((node = zmalloc(sizeof(*node))) == NULL)
        return NULL;
    node->value = value;
    if (after) {
        node->prev = old_node;
        node->next = old_node->next;
        if (list->tail == old_node) {
            list->tail = node;
        }
    } else {
        node->next = old_node;
        node->prev = old_node->prev;
        if (list->head == old_node) {
            list->head = node;
        }
    }
    if (node->prev != NULL) {
        node->prev->next = node;
    }
    if (node->next != NULL) {
        node->next->prev = node;
    }
    list->len++;
    return list;
}

//// 删除节点
void listDelNode(list *list, listNode *node)
{
    if (node->prev)                         // 删除节点不为头节点
        node->prev->next = node->next;
    else                                    // 删除节点为头节点需要改变head的指向
        list->head = node->next;

    if (node->next)                         // 删除节点不为尾节点
        node->next->prev = node->prev;
    else                                    // 删除节点为尾节点需要改变tail的指向
        list->tail = node->prev;

    //// 释放
    if (list->free) list->free(node->value);// 释放节点值
    zfree(node);                            // 释放节点
    list->len--;                            // 长度-1
}

//// 获取迭代器
listIter *listGetIterator(list *list, int direction)
{
    listIter *iter;                 // 声明迭代器

    if ((iter = zmalloc(sizeof(*iter))) == NULL) return NULL;
    // 根据迭代方向来初始化iter
    if (direction == AL_START_HEAD)
        iter->next = list->head;
    else
        iter->next = list->tail;
    iter->direction = direction;
    return iter;
}


//// 释放迭代器
void listReleaseIterator(listIter *iter) {
    zfree(iter);
}


//// 重置正向迭代器
void listRewind(list *list, listIter *li) {
    li->next = list->head;
    li->direction = AL_START_HEAD;
}


//// 重置逆向迭代器
void listRewindTail(list *list, listIter *li) {
    li->next = list->tail;
    li->direction = AL_START_TAIL;
}


//// 根据迭代器获取一个节点
listNode *listNext(listIter *iter)
{
    listNode *current = iter->next;

    if (current != NULL) {
        if (iter->direction == AL_START_HEAD)
            iter->next = current->next;
        else
            iter->next = current->prev;
    }
    return current;
}

//// 复制链表
list *listDup(list *orig)
{
    list *copy;
    listIter iter;
    listNode *node;

    if ((copy = listCreate()) == NULL)
        return NULL;
    //// 复制节点值操作函数
    copy->dup = orig->dup;
    copy->free = orig->free;
    copy->match = orig->match;

    //// 用正向迭代器来遍历节点
    listRewind(orig, &iter);
    while((node = listNext(&iter)) != NULL) {
        void *value;

        if (copy->dup) {
            value = copy->dup(node->value);
            if (value == NULL) {
                listRelease(copy);
                return NULL;
            }
        } else
            value = node->value;
        if (listAddNodeTail(copy, value) == NULL) {
            listRelease(copy);
            return NULL;
        }
    }
    return copy;
}

//// 根据给定节点值，在链表中查找该节点，比较的是指针（地址）
listNode *listSearchKey(list *list, void *key)
{
    listIter iter;
    listNode *node;

    listRewind(list, &iter);
    while((node = listNext(&iter)) != NULL) {           // 遍历整个链表O(n)
        if (list->match) {
            if (list->match(node->value, key)) {        // 如果定义了match匹配函数，则利用该函数进行节点匹配
                return node;
            }
        } else {                                        // 如果没有定义match，则直接比较节点值
            if (key == node->value) {                   // 找到该节点
                return node;
            }
        }
    }
    return NULL;
}

//// 根据序号来查找节点
listNode *listIndex(list *list, long index) {
    listNode *n;

    if (index < 0) {                            // 序号为负，则倒序查找
        index = (-index)-1;
        n = list->tail;
        while(index-- && n) n = n->prev;
    } else {                                    // 正序查找
        n = list->head;
        while(index-- && n) n = n->next;
    }
    return n;
}

//// 旋转函数
void listRotate(list *list) {
    listNode *tail = list->tail;

    if (listLength(list) <= 1) return;

    // 取出表尾指针
    list->tail = tail->prev;
    list->tail->next = NULL;

    // 将其移动到表头并成为新的表头指针
    list->head->prev = tail;
    tail->prev = NULL;
    tail->next = list->head;
    list->head = tail;
}

//// 合并链表，将合并到l上，清空o
void listJoin(list *l, list *o) {
    if (o->head)
        o->head->prev = l->tail;

    if (l->tail)
        l->tail->next = o->head;
    else
        l->head = o->head;

    l->tail = o->tail;
    l->len += o->len;

    /* Setup other as an empty list. */
    o->head = o->tail = NULL;
    o->len = 0;
}
