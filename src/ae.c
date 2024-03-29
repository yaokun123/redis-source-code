#include <stdio.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>
#include <stdlib.h>
#include <poll.h>
#include <string.h>
#include <time.h>
#include <errno.h>

#include "ae.h"
#include "zmalloc.h"
#include "config.h"

//// 包括本系统所支持的最佳多路复用层以下应按性能降序排列
// linux下可以使用select和epoll
// macos下可以使用select和kqueue
// 如果安装有libevent，则可以使用evport
#ifdef HAVE_EVPORT
#include "ae_evport.c"
#else
    #ifdef HAVE_EPOLL
    #include "ae_epoll.c"
    #else
        #ifdef HAVE_KQUEUE
        #include "ae_kqueue.c"
        #else
        #include "ae_select.c"
        #endif
    #endif
#endif

//// 创建事件循环结构体，在server.c的initServer中调用
aeEventLoop *aeCreateEventLoop(int setsize) {
    aeEventLoop *eventLoop;
    int i;

    if ((eventLoop = zmalloc(sizeof(*eventLoop))) == NULL) goto err;
    eventLoop->events = zmalloc(sizeof(aeFileEvent)*setsize);   // 保存要使用的文件事件
    eventLoop->fired = zmalloc(sizeof(aeFiredEvent)*setsize);   // 保存已准备好，待处理的文件事件
    if (eventLoop->events == NULL || eventLoop->fired == NULL) goto err;

    eventLoop->setsize = setsize;                                   // 需要监听的描述符个数
    eventLoop->lastTime = time(NULL);                               // 上一次事件循环的时间，用于检测系统时间是否变更
    eventLoop->timeEventHead = NULL;                                // 时间事件头，因为事件时间其实是一个链表
    eventLoop->timeEventNextId = 0;                                 // 下一个时间事件的id，用于生成时间事件的唯一标识
    eventLoop->stop = 0;                                            // 停止标识，1表示停止
    eventLoop->maxfd = -1;                                          // 当前注册的最大描述符

    eventLoop->beforesleep = NULL;
    eventLoop->aftersleep = NULL;


    if (aeApiCreate(eventLoop) == -1) goto err;                     // 初始化文件事件管理器（比如epoll_create）

    // 初始文件事件的存储空间，将事件类型置为0
    for (i = 0; i < setsize; i++)
        eventLoop->events[i].mask = AE_NONE;
    return eventLoop;

err:
    if (eventLoop) {
        zfree(eventLoop->events);
        zfree(eventLoop->fired);
        zfree(eventLoop);
    }
    return NULL;
}


//// 返回当前事件循环的setsize
int aeGetSetSize(aeEventLoop *eventLoop) {
    return eventLoop->setsize;
}


//// 扩容/缩容 当前事件循环的setsize
int aeResizeSetSize(aeEventLoop *eventLoop, int setsize) {
    int i;

    if (setsize == eventLoop->setsize) return AE_OK;
    if (eventLoop->maxfd >= setsize) return AE_ERR;
    if (aeApiResize(eventLoop,setsize) == -1) return AE_ERR;

    eventLoop->events = zrealloc(eventLoop->events,sizeof(aeFileEvent)*setsize);
    eventLoop->fired = zrealloc(eventLoop->fired,sizeof(aeFiredEvent)*setsize);
    eventLoop->setsize = setsize;

    /* Make sure that if we created new slots, they are initialized with
     * an AE_NONE mask. */
    for (i = eventLoop->maxfd+1; i < setsize; i++)
        eventLoop->events[i].mask = AE_NONE;
    return AE_OK;
}


//// 删除事件循环
void aeDeleteEventLoop(aeEventLoop *eventLoop) {
    aeApiFree(eventLoop);
    zfree(eventLoop->events);
    zfree(eventLoop->fired);
    zfree(eventLoop);
}


//// 停止事件循环
void aeStop(aeEventLoop *eventLoop) {
    eventLoop->stop = 1;
}

//// 创建文件事件
// 关于文件事件，有两个地方需要创建它
// 1、初始化服务器的时候，需要监听新的客户连接           server.c/initServer ->  acceptTcpHandler操作函数
// 2、在客户连接服务器之后，需要监听该客户的读写事件      etWorking.c/createClient  ->  readQueryFromClient操作函数
// 3、aof重写时候父子进程间使用管道通信                aof.c/aofCreatePipes  -> aofChildPipeReadable操作函数
// 4、aof                                        aof.c/aofRewriteBufferAppend -> aofChildWriteDiffData操作函数
int aeCreateFileEvent(aeEventLoop *eventLoop, int fd, int mask,
        aeFileProc *proc, void *clientData)
{
    if (fd >= eventLoop->setsize) {                 // 超出监听的描述符个数，返回错误
        errno = ERANGE;
        return AE_ERR;
    }
    aeFileEvent *fe = &eventLoop->events[fd];       // 注册文件事件使用文件描述符作为数组索引

    if (aeApiAddEvent(eventLoop, fd, mask) == -1)   // 向系统注册事件，调用redis封装好的多路复用io管理器（比如加入到epool_ctl）
        return AE_ERR;
    fe->mask |= mask;                               // 设置文件事件的事件标记，读or写
    if (mask & AE_READABLE) fe->rfileProc = proc;   // 设置文件事件的处理函数 - 读
    if (mask & AE_WRITABLE) fe->wfileProc = proc;   // 设置文件事件的处理函数 - 写
    fe->clientData = clientData;
    if (fd > eventLoop->maxfd)
        eventLoop->maxfd = fd;                      // 更新监听的最大文件描述符
    return AE_OK;
}


//// 删除文件事件
void aeDeleteFileEvent(aeEventLoop *eventLoop, int fd, int mask)
{
    if (fd >= eventLoop->setsize) return;               // 如果传入的文件描述符超过监听的最大文件描述符，就没必要再删除了。直接返回
    aeFileEvent *fe = &eventLoop->events[fd];           // 根据文件描述符取出文件事件
    if (fe->mask == AE_NONE) return;                    // 如果文件事件的事件标记为NONE，就没比较删除。直接返回

    aeApiDelEvent(eventLoop, fd, mask);                 // 从系统删除事件
    fe->mask = fe->mask & (~mask);                      // 设置文件事件的事件标记为NONE，标志该事件已经删除


    // 如果当前的文件描述符 == 事件循环监听的最大文件描述符。则要更新监听的最大文件描述符
    if (fd == eventLoop->maxfd && fe->mask == AE_NONE) {
        int j;

        // 遍历监听的文件描述符数组，从后往前的方式，直到找到一个有效的文件描述符(make != NONE)
        for (j = eventLoop->maxfd-1; j >= 0; j--)
            if (eventLoop->events[j].mask != AE_NONE) break;
        eventLoop->maxfd = j;
    }
}


//// 根据文件描述符获取文件事件（读/写/无）
int aeGetFileEvents(aeEventLoop *eventLoop, int fd) {
    if (fd >= eventLoop->setsize) return 0;             // 校验文件描述符有没有超过事件循环监听的最大文件描述符
    aeFileEvent *fe = &eventLoop->events[fd];           // 根据文件描述符，取出文件事件

    return fe->mask;                                    // 返回文件事件的事件标记，读、写、NONE
}


//// 获取当前时间(秒/毫秒)
static void aeGetTime(long *seconds, long *milliseconds)
{
    struct timeval tv;

    gettimeofday(&tv, NULL);
    *seconds = tv.tv_sec;
    *milliseconds = tv.tv_usec/1000;
}


//// 处理时间事件的执行时间（秒/毫秒）
static void aeAddMillisecondsToNow(long long milliseconds, long *sec, long *ms) {
    long cur_sec, cur_ms, when_sec, when_ms;

    // 获取当前时间（秒/毫秒）
    aeGetTime(&cur_sec, &cur_ms);

    when_sec = cur_sec + milliseconds/1000;
    when_ms = cur_ms + milliseconds%1000;
    if (when_ms >= 1000) {
        when_sec ++;
        when_ms -= 1000;
    }
    *sec = when_sec;
    *ms = when_ms;
}


//// 创建时间事件     server.c/initServer  ->  serverCron操作函数
long long aeCreateTimeEvent(aeEventLoop *eventLoop, long long milliseconds,
        aeTimeProc *proc, void *clientData,
        aeEventFinalizerProc *finalizerProc)
{
    long long id = eventLoop->timeEventNextId++;    // 下一个时间事件的id++
    aeTimeEvent *te;

    te = zmalloc(sizeof(*te));                      // 分配空间
    if (te == NULL) return AE_ERR;                  // 分配失败，直接返回
    te->id = id;                                    // 分配时间事件的唯一标识id

    // when_sec = 1638587231
    // when_ms = 306
    aeAddMillisecondsToNow(milliseconds,&te->when_sec,&te->when_ms);// 设置时间事件的执行时间，秒/毫秒

    te->timeProc = proc;                            // 设置时间事件对应的处理函数
    te->finalizerProc = finalizerProc;              // 时间事件的最后一次处理程序，若已设置，则删除该事件的时候会调用
    te->clientData = clientData;                    // 数据

    // 头插法维护时间事件单链表
    te->next = eventLoop->timeEventHead;
    eventLoop->timeEventHead = te;
    return id;
}


//// 根据创建时间事件时返回的唯一id，删除时间事件
// 该方法在redis中没有使用
int aeDeleteTimeEvent(aeEventLoop *eventLoop, long long id)
{
    aeTimeEvent *te = eventLoop->timeEventHead;     // 取出时间事件单链表的头部地址

    // 循环遍历链表
    while(te) {
        if (te->id == id) {                         // 根据事件事件分配的唯一id
            te->id = AE_DELETED_EVENT_ID;           // 找到之后将id设置为-1，但是并没有从单链表中真正删除
            return AE_OK;
        }
        te = te->next;
    }
    return AE_ERR; /* NO event with the specified ID found */
}


//// 获取最近的时间事件
static aeTimeEvent *aeSearchNearestTimer(aeEventLoop *eventLoop)
{
    aeTimeEvent *te = eventLoop->timeEventHead;                     // 取出时间事件单链表的头部地址
    aeTimeEvent *nearest = NULL;                                    // 存放最近的时间事件

    // 循环遍历时间事件链表，找出最近要执行的事件
    while(te) {
        if (!nearest || te->when_sec < nearest->when_sec ||
                (te->when_sec == nearest->when_sec && te->when_ms < nearest->when_ms))
            nearest = te;
        te = te->next;
    }
    return nearest;
}


//// 处理时间事件
static int processTimeEvents(aeEventLoop *eventLoop) {
    int processed = 0;
    aeTimeEvent *te, *prev;
    long long maxId;
    time_t now = time(NULL);                // 获取当前时间


    if (now < eventLoop->lastTime) {        // 事件循环处理的最后时间与当前时间比较
        te = eventLoop->timeEventHead;
        while(te) {
            te->when_sec = 0;
            te = te->next;
        }
    }
    eventLoop->lastTime = now;              // 更新循环处理的最后时间为当前时间

    prev = NULL;
    te = eventLoop->timeEventHead;          // 取出时间事件头指针
    maxId = eventLoop->timeEventNextId-1;
    while(te) {
        long now_sec, now_ms;
        long long id;

        /* Remove events scheduled for deletion. */
        if (te->id == AE_DELETED_EVENT_ID) {// 时间事件已经被删除，删除时间时间节点
            aeTimeEvent *next = te->next;
            if (prev == NULL)
                eventLoop->timeEventHead = te->next;
            else
                prev->next = te->next;
            if (te->finalizerProc)
                te->finalizerProc(eventLoop, te->clientData);
            zfree(te);
            te = next;
            continue;
        }


        // 跳过无效事件
        if (te->id > maxId) {
            te = te->next;
            continue;
        }
        aeGetTime(&now_sec, &now_ms);

        // 如果当前时间等于或等于事件的执行时间，那么说明事件已到达，执行这个事件
        if (now_sec > te->when_sec ||
            (now_sec == te->when_sec && now_ms >= te->when_ms))
        {
            int retval;

            id = te->id;
            retval = te->timeProc(eventLoop, id, te->clientData);   //// 处理时间事件
            processed++;

            //// 记录是否有需要循环执行这个事件时间
            if (retval != AE_NOMORE) {
                // 是的， retval 毫秒之后继续执行这个时间事件
                aeAddMillisecondsToNow(retval,&te->when_sec,&te->when_ms);
            } else {
                // 不，将这个事件删除
                te->id = AE_DELETED_EVENT_ID;
            }
        }
        prev = te;
        te = te->next;
    }
    return processed;
}


//// 文件事件/事件事件 处理函数，redis主进程循环执行该函数
int aeProcessEvents(aeEventLoop *eventLoop, int flags)
{
    int processed = 0, numevents;

    // 没有需要处理的事件就直接返回
    if (!(flags & AE_TIME_EVENTS) && !(flags & AE_FILE_EVENTS)) return 0;

    // 即使没有要处理的文件事件，只要我们想处理时间事件就需要调用select()函数
    // 这是为了睡眠直到下一个时间事件准备好。
    if (eventLoop->maxfd != -1 ||
        ((flags & AE_TIME_EVENTS) && !(flags & AE_DONT_WAIT))) {
        int j;
        aeTimeEvent *shortest = NULL;
        struct timeval tv, *tvp;

        // 获取最近的时间事件
        if (flags & AE_TIME_EVENTS && !(flags & AE_DONT_WAIT))
            shortest = aeSearchNearestTimer(eventLoop);


        if (shortest) {
            //// 运行到这里说明最近需要执行的时间事件存在，则根据最近可执行时间事件和现在的时间的时间差来决定
            long now_sec, now_ms;

            aeGetTime(&now_sec, &now_ms);       // 获取当前时间(秒/毫秒)
            tvp = &tv;

            //// 计算下一次时间事件要执行的时间差，文件事件管理器（比如epoll_wait）要等待的时间
            //// 不用担心这个时间差会太大，因为serverCron会每100ms执行一次
            long long ms =
                (shortest->when_sec - now_sec)*1000 +
                shortest->when_ms - now_ms;

            if (ms > 0) {
                tvp->tv_sec = ms/1000;
                tvp->tv_usec = (ms % 1000)*1000;
            } else {
                // 时间差小于0，代表可以处理了
                tvp->tv_sec = 0;
                tvp->tv_usec = 0;
            }
        } else {
            //// 执行到这里，说明没有待处理的时间事件，此时根据AE_DONT_WAIT参数来决定是否设置阻塞和阻塞的时间
            if (flags & AE_DONT_WAIT) {
                tv.tv_sec = tv.tv_usec = 0;
                tvp = &tv;
            } else {
                /* Otherwise we can block */
                tvp = NULL; /* wait forever */
            }
        }

        //// 调用I/O复用函数获取已准备好的事件
        numevents = aeApiPoll(eventLoop, tvp);

        // 回调函数
        if (eventLoop->aftersleep != NULL && flags & AE_CALL_AFTER_SLEEP)
            eventLoop->aftersleep(eventLoop);

        // 遍历已经准备好的文件事件
        for (j = 0; j < numevents; j++) {
            // 从已就绪事件中获取事件
            aeFileEvent *fe = &eventLoop->events[eventLoop->fired[j].fd];
            int mask = eventLoop->fired[j].mask;
            int fd = eventLoop->fired[j].fd;
            int rfired = 0;

            // 如果为读事件则调用读事件处理函数
            if (fe->mask & mask & AE_READABLE) {
                rfired = 1;
                fe->rfileProc(eventLoop,fd,fe->clientData,mask);
            }

            // 如果为写事件则调用写事件处理函数
            if (fe->mask & mask & AE_WRITABLE) {
                if (!rfired || fe->wfileProc != fe->rfileProc)
                    fe->wfileProc(eventLoop,fd,fe->clientData,mask);
            }
            processed++;
        }
    }

    //// 处理时间事件，此处说明Redis的文件事件优先于时间事件
    if (flags & AE_TIME_EVENTS)
        processed += processTimeEvents(eventLoop);

    return processed; // 返回处理的事件个数
}

/* Wait for milliseconds until the given file descriptor becomes
 * writable/readable/exception */
int aeWait(int fd, int mask, long long milliseconds) {
    struct pollfd pfd;
    int retmask = 0, retval;

    memset(&pfd, 0, sizeof(pfd));
    pfd.fd = fd;
    if (mask & AE_READABLE) pfd.events |= POLLIN;
    if (mask & AE_WRITABLE) pfd.events |= POLLOUT;

    if ((retval = poll(&pfd, 1, milliseconds))== 1) {
        if (pfd.revents & POLLIN) retmask |= AE_READABLE;
        if (pfd.revents & POLLOUT) retmask |= AE_WRITABLE;
	if (pfd.revents & POLLERR) retmask |= AE_WRITABLE;
        if (pfd.revents & POLLHUP) retmask |= AE_WRITABLE;
        return retmask;
    } else {
        return retval;
    }
}

//// 事件循环主函数，在server.c的main函数中调用，redis主进程最终是在循环执行事件
void aeMain(aeEventLoop *eventLoop) {
    eventLoop->stop = 0;    // 开启事件循环
    while (!eventLoop->stop) {
        // 如果beforesleep被设置则先执行
        if (eventLoop->beforesleep != NULL)
            eventLoop->beforesleep(eventLoop);

        // 事件处理函数
        aeProcessEvents(eventLoop, AE_ALL_EVENTS|AE_CALL_AFTER_SLEEP);
    }
}

//// 返回io多路复用器的名称
char *aeGetApiName(void) {
    return aeApiName();
}

//// 设置beforesleep
void aeSetBeforeSleepProc(aeEventLoop *eventLoop, aeBeforeSleepProc *beforesleep) {
    eventLoop->beforesleep = beforesleep;
}

//// 设置aftersleep
void aeSetAfterSleepProc(aeEventLoop *eventLoop, aeBeforeSleepProc *aftersleep) {
    eventLoop->aftersleep = aftersleep;
}
