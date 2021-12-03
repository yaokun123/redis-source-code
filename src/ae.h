/* A simple event-driven programming library. Originally I wrote this code
 * for the Jim's event-loop (Jim is a Tcl interpreter) but later translated
 * it in form of a library for easy reuse.
 *
 * Copyright (c) 2006-2012, Salvatore Sanfilippo <antirez at gmail dot com>
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of Redis nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#ifndef __AE_H__
#define __AE_H__

#include <time.h>

#define AE_OK 0
#define AE_ERR -1

#define AE_NONE 0
#define AE_READABLE 1
#define AE_WRITABLE 2

#define AE_FILE_EVENTS 1
#define AE_TIME_EVENTS 2
#define AE_ALL_EVENTS (AE_FILE_EVENTS|AE_TIME_EVENTS)
#define AE_DONT_WAIT 4
#define AE_CALL_AFTER_SLEEP 8

#define AE_NOMORE -1
#define AE_DELETED_EVENT_ID -1

/* Macros */
#define AE_NOTUSED(V) ((void) V)

struct aeEventLoop;

/* Types and data structures */
typedef void aeFileProc(struct aeEventLoop *eventLoop, int fd, void *clientData, int mask);
typedef int aeTimeProc(struct aeEventLoop *eventLoop, long long id, void *clientData);
typedef void aeEventFinalizerProc(struct aeEventLoop *eventLoop, void *clientData);
typedef void aeBeforeSleepProc(struct aeEventLoop *eventLoop);

// 文件事件结构体
typedef struct aeFileEvent {
    int mask;                   // 事件标记，读or写
    aeFileProc *rfileProc;      // 读事件处理函数
    aeFileProc *wfileProc;      // 写事件处理函数
    void *clientData;           // 事件中包含的待处理数据
} aeFileEvent;

// 时间事件结构体
typedef struct aeTimeEvent {
    long long id;               // 时间事件标识符，用于唯一标记该时间事件
    long when_sec;              // 时间事件触发时间 秒
    long when_ms;               // 时间事件触发时间 微秒
    aeTimeProc *timeProc;       // 该事件对应的处理函数
    aeEventFinalizerProc *finalizerProc;// 时间事件的最后一次处理程序，若已设置，则删除该事件的时候会调用
    void *clientData;           // 数据
    struct aeTimeEvent *next;   // 下一个时间事件
} aeTimeEvent;

// 触发时间结构体
typedef struct aeFiredEvent {
    int fd;                     // 文件事件描述符
    int mask;                   // 读写标记
} aeFiredEvent;

// 每次调用I/O复用程序之后，会返回已经准备好的文件事件描述符，这时候就会以该结构体的形式存放下来
// Redis在事件循环的时候，会对这些已准备好待处理的事件一一进行处理，也就是上面说的待处理事件池
typedef struct aeEventLoop {
    int maxfd;                  // 当前注册的最大描述符
    int setsize;                // 需要监听的描述符个数
    long long timeEventNextId;  // 下一个时间事件的id，用于生成时间事件的唯一标识
    time_t lastTime;            // 上一次事件循环的时间，用于检测系统时间是否变更
    aeFileEvent *events;        // 注册要使用的文件事件
    aeFiredEvent *fired;        // 已准备好，待处理的时间
    aeTimeEvent *timeEventHead; // 时间事件头，因为事件时间其实是一个链表
    int stop;                   // 停止标识，1表示停止
    void *apidata;              // 用于处理底层特定的API数据，对于epoll来说，其包括epoll_fd和epoll_event
    aeBeforeSleepProc *beforesleep;// 没有待处理事件时调用
    aeBeforeSleepProc *aftersleep;
} aeEventLoop;

/* Prototypes */
aeEventLoop *aeCreateEventLoop(int setsize);
void aeDeleteEventLoop(aeEventLoop *eventLoop);
void aeStop(aeEventLoop *eventLoop);
int aeCreateFileEvent(aeEventLoop *eventLoop, int fd, int mask,
        aeFileProc *proc, void *clientData);
void aeDeleteFileEvent(aeEventLoop *eventLoop, int fd, int mask);
int aeGetFileEvents(aeEventLoop *eventLoop, int fd);
long long aeCreateTimeEvent(aeEventLoop *eventLoop, long long milliseconds,
        aeTimeProc *proc, void *clientData,
        aeEventFinalizerProc *finalizerProc);
int aeDeleteTimeEvent(aeEventLoop *eventLoop, long long id);
int aeProcessEvents(aeEventLoop *eventLoop, int flags);
int aeWait(int fd, int mask, long long milliseconds);
void aeMain(aeEventLoop *eventLoop);
char *aeGetApiName(void);
void aeSetBeforeSleepProc(aeEventLoop *eventLoop, aeBeforeSleepProc *beforesleep);
void aeSetAfterSleepProc(aeEventLoop *eventLoop, aeBeforeSleepProc *aftersleep);
int aeGetSetSize(aeEventLoop *eventLoop);
int aeResizeSetSize(aeEventLoop *eventLoop, int setsize);

#endif
