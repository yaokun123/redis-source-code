#include "server.h"
#include "bio.h"
#include "atomicvar.h"
#include "cluster.h"

static size_t lazyfree_objects = 0;
pthread_mutex_t lazyfree_objects_mutex = PTHREAD_MUTEX_INITIALIZER;

/* Return the number of currently pending objects to free. */
size_t lazyfreeGetPendingObjectsCount(void) {
    size_t aux;
    atomicGet(lazyfree_objects,aux);
    return aux;
}

size_t lazyfreeGetFreeEffort(robj *obj) {
    if (obj->type == OBJ_LIST) { // 如果是list，返回list的元素个数
        quicklist *ql = obj->ptr;
        return ql->len;
    } else if (obj->type == OBJ_SET && obj->encoding == OBJ_ENCODING_HT) { // 如果是set，返回set的元素个数
        dict *ht = obj->ptr;
        return dictSize(ht);
    } else if (obj->type == OBJ_ZSET && obj->encoding == OBJ_ENCODING_SKIPLIST){ // 如果是zset，返回zset的元素个数
        zset *zs = obj->ptr;
        return zs->zsl->length;
    } else if (obj->type == OBJ_HASH && obj->encoding == OBJ_ENCODING_HT) { // 如果是dict，返回dict的元素个数
        dict *ht = obj->ptr;
        return dictSize(ht);
    } else {                                                                // 其他情况返回1
        return 1;
    }
}

//// 异步删除key
#define LAZYFREE_THRESHOLD 64
int dbAsyncDelete(redisDb *db, robj *key) {
    // 在过期字典中删除key
    if (dictSize(db->expires) > 0) dictDelete(db->expires,key->ptr);

    // 在数据库中异步删除key
    dictEntry *de = dictUnlink(db->dict,key->ptr);
    if (de) {

        robj *val = dictGetVal(de);                                         // 获取到key对应的val
        size_t free_effort = lazyfreeGetFreeEffort(val);                    // 获取val的元素个数

        // 如果要释放对象的元素太多，将会放入异步删除的队列中。此时虽然没有删除，但是会讲key的val设置为NULL
        if (free_effort > LAZYFREE_THRESHOLD) {
            atomicIncr(lazyfree_objects,1);
            bioCreateBackgroundJob(BIO_LAZY_FREE,val,NULL,NULL); // 加入异步处理队列，type=2
                                                                           // listAddNodeTail(bio_jobs[type],job);
            dictSetVal(db->dict,de,NULL);                               // 将key对应的val设置为空
        }
    }

    // 如果要释放对象的元素不多，
    if (de) {
        dictFreeUnlinkedEntry(db->dict,de);
        if (server.cluster_enabled) slotToKeyDel(key);
        return 1;
    } else {
        return 0;
    }
}

/* Empty a Redis DB asynchronously. What the function does actually is to
 * create a new empty set of hash tables and scheduling the old ones for
 * lazy freeing. */
void emptyDbAsync(redisDb *db) {
    dict *oldht1 = db->dict, *oldht2 = db->expires;
    db->dict = dictCreate(&dbDictType,NULL);
    db->expires = dictCreate(&keyptrDictType,NULL);
    atomicIncr(lazyfree_objects,dictSize(oldht1));
    bioCreateBackgroundJob(BIO_LAZY_FREE,NULL,oldht1,oldht2);
}

/* Empty the slots-keys map of Redis CLuster by creating a new empty one
 * and scheduiling the old for lazy freeing. */
void slotToKeyFlushAsync(void) {
    rax *old = server.cluster->slots_to_keys;

    server.cluster->slots_to_keys = raxNew();
    memset(server.cluster->slots_keys_count,0,
           sizeof(server.cluster->slots_keys_count));
    atomicIncr(lazyfree_objects,old->numele);
    bioCreateBackgroundJob(BIO_LAZY_FREE,NULL,NULL,old);
}

/* Release objects from the lazyfree thread. It's just decrRefCount()
 * updating the count of objects to release. */
void lazyfreeFreeObjectFromBioThread(robj *o) {
    decrRefCount(o);
    atomicDecr(lazyfree_objects,1);
}

/* Release a database from the lazyfree thread. The 'db' pointer is the
 * database which was substitutied with a fresh one in the main thread
 * when the database was logically deleted. 'sl' is a skiplist used by
 * Redis Cluster in order to take the hash slots -> keys mapping. This
 * may be NULL if Redis Cluster is disabled. */
void lazyfreeFreeDatabaseFromBioThread(dict *ht1, dict *ht2) {
    size_t numkeys = dictSize(ht1);
    dictRelease(ht1);
    dictRelease(ht2);
    atomicDecr(lazyfree_objects,numkeys);
}

/* Release the skiplist mapping Redis Cluster keys to slots in the
 * lazyfree thread. */
void lazyfreeFreeSlotsMapFromBioThread(rax *rt) {
    size_t len = rt->numele;
    raxFree(rt);
    atomicDecr(lazyfree_objects,len);
}
