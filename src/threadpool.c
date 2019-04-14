/*
 * Copyright (c) 2016, Mathias Brossard <mathias@brossard.org>.
 * All rights reserved.
 * 
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are
 * met:
 * 
 *  1. Redistributions of source code must retain the above copyright
 *     notice, this list of conditions and the following disclaimer.
 * 
 *  2. Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

/**
 * @file threadpool.c
 * @brief Threadpool implementation file
 */

#include <stdlib.h>
#include <pthread.h>
#include <unistd.h>

#include "threadpool.h"

typedef enum {
    immediate_shutdown = 1,
    graceful_shutdown  = 2
} threadpool_shutdown_t;//线程池关闭类型

/**
 *  @struct threadpool_task
 *  @brief the work struct
 *
 *  @var function Pointer to the function that will perform the task.
 *  @var argument Argument to be passed to the function.
 */

typedef struct {
    void (*function)(void *); //任务函数的函数指针
    void *argument; //任务函数的参数
} threadpool_task_t;

/**
 *  @struct threadpool
 *  @brief The threadpool struct
 *
 *  @var notify       Condition variable to notify worker threads.
 *  @var threads      Array containing worker threads ID.
 *  @var thread_count Number of threads
 *  @var queue        Array containing the task queue.
 *  @var queue_size   Size of the task queue.
 *  @var head         Index of the first element.
 *  @var tail         Index of the next element.
 *  @var count        Number of pending tasks
 *  @var shutdown     Flag indicating if the pool is shutting down
 *  @var started      Number of started threads
 */
struct threadpool_t {
  pthread_mutex_t lock;         //互斥锁
  pthread_cond_t notify;        //条件变量
  pthread_t *threads;           //线程池数组首指针
  threadpool_task_t *queue;     //任务队列数组首指针
  int thread_count;             //线程总数量
  int queue_size;               //任务总数量
  int head;                     //当前任务队列的头部
  int tail;                     //当前任务队列的尾部
  int count;                    //待运行的任务
  int shutdown;                 //线程池状态是否关闭
  int started;                  //正在运行的线程数
};

/**
 * @function void *threadpool_thread(void *threadpool)
 * @brief the worker thread
 * @param threadpool the pool which own the thread
 */
static void *threadpool_thread(void *threadpool);

int threadpool_free(threadpool_t *pool);//释放资源

threadpool_t *threadpool_create(int thread_count, int queue_size, int flags)
{//创建一个线程池对象；线程数量；任务数量；是否为新创建的线程
    threadpool_t *pool;
    int i;
    (void) flags;

    if(thread_count <= 0 || thread_count > MAX_THREADS || queue_size <= 0 || queue_size > MAX_QUEUE) {
        //判断数量是否合格
        return NULL;
    }

    if((pool = (threadpool_t *)malloc(sizeof(threadpool_t))) == NULL) {//分配线程池内存空间
        goto err;
    }

    /* Initialize 初始化*/
    pool->thread_count = 0;
    pool->queue_size = queue_size;
    pool->head = pool->tail = pool->count = 0;
    pool->shutdown = pool->started = 0;

    /* Allocate thread and task queue */
    pool->threads = (pthread_t *)malloc(sizeof(pthread_t) * thread_count);//分配线程数组内存空间
    pool->queue = (threadpool_task_t *)malloc
        (sizeof(threadpool_task_t) * queue_size);//分配任务队列数组内存空间

    /* Initialize mutex and conditional variable first 初始化互斥锁和条件变量 */
    if((pthread_mutex_init(&(pool->lock), NULL) != 0) ||
       (pthread_cond_init(&(pool->notify), NULL) != 0) ||
       (pool->threads == NULL) ||
       (pool->queue == NULL)) {
        goto err;
    }

    /* Start worker threads */
    for(i = 0; i < thread_count; i++) {
        if(pthread_create(&(pool->threads[i]), NULL,
                          threadpool_thread, (void*)pool) != 0) {
            //创建thread_count个线程，并在线程池的thread数组中存储tid，线程创建成功时就执行threadpool_thread
            threadpool_destroy(pool, 0);//创建失败则销毁线程
            return NULL;
        }
        pool->thread_count++;
        pool->started++;
    }

    return pool;//创建完毕之后当前运行的头尾都为0

 err:
    if(pool) {
        threadpool_free(pool);
    }
    return NULL;
}

int threadpool_add(threadpool_t *pool, void (*function)(void *),
                   void *argument, int flags)
{//向任务队列中添加新任务
    int err = 0;
    int next;
    (void) flags;

    if(pool == NULL || function == NULL) {//有空指针则返回错误
        return threadpool_invalid;
    }

    if(pthread_mutex_lock(&(pool->lock)) != 0) {//互斥访问条件变量，加锁
        return threadpool_lock_failure;
    }

    next = (pool->tail + 1) % pool->queue_size;//计算可以下一个可以存放task的位置

    do {
        /* Are we full ? */
        if(pool->count == pool->queue_size) {//待执行的任务等于队列长度，任务队列是满的，不能添加新任务
            err = threadpool_queue_full;
            break;
        }

        /* Are we shutting down ? */
        if(pool->shutdown) {//检查线程池是否是关闭的
            err = threadpool_shutdown;
            break;
        }

        /* Add task to queue *///将任务加入任务队列
        pool->queue[pool->tail].function = function;
        pool->queue[pool->tail].argument = argument;
        pool->tail = next;
        pool->count += 1;

        /* pthread_cond_broadcast *///每添加一个任务就通过条件变量通知threadpool_thread
        if(pthread_cond_signal(&(pool->notify)) != 0) {
            err = threadpool_lock_failure;
            break;
        }
    } while(0);

    if(pthread_mutex_unlock(&pool->lock) != 0) {//解锁条件变量
        err = threadpool_lock_failure;
    }

    return err;
}

int threadpool_destroy(threadpool_t *pool, int flags)
{//销毁线程池
    int i, err = 0;

    if(pool == NULL) {//线程池为空返回线程池无效
        return threadpool_invalid;
    }

    if(pthread_mutex_lock(&(pool->lock)) != 0) {//获取互斥锁，为了使用224行使用条件变量
        return threadpool_lock_failure;
    }

    do {
        /* Already shutting down 如果已经关闭，返回已关闭错误*/
        if(pool->shutdown) {
            err = threadpool_shutdown;
            break;
        }

        pool->shutdown = (flags & threadpool_graceful) ?//获取指定的关闭方式
            graceful_shutdown : immediate_shutdown;

        /* Wake up all worker threads */
        if((pthread_cond_broadcast(&(pool->notify)) != 0) ||//有些线程可能因为任务队列为空阻塞在条件变量上，唤醒，然后解锁
           (pthread_mutex_unlock(&(pool->lock)) != 0)) {
            err = threadpool_lock_failure;
            break;
        }

        /* Join all worker thread */
        for(i = 0; i < pool->thread_count; i++) {//阻塞等待所有的线程终止，失败返回threadpool_thread_failure
            if(pthread_join(pool->threads[i], NULL) != 0) {
                err = threadpool_thread_failure;
            }
        }
    } while(0);

    /* Only if everything went well do we deallocate the pool */
    if(!err) {//释放内存资源
        threadpool_free(pool);
    }
    return err;
}

int threadpool_free(threadpool_t *pool)
{
    if(pool == NULL || pool->started > 0) {//如果线程池不存在或者线程池中仍然有运行的线程，则不能释放内存
        return -1;
    }

    /* Did we manage to allocate ? */
    if(pool->threads) {//如果线程池数组存在，就释放线程池数组和任务队列数组
        free(pool->threads);
        free(pool->queue);
 
        /* Because we allocate pool->threads after initializing the
           mutex and condition variable, we're sure they're
           initialized. Let's lock the mutex just in case.
           在创建线程池时，先初始化互斥锁，后分配线程池数组和任务队列数组的内存，所以不用判断是否互斥锁的存在
           为了以防万一，先锁着互斥锁，然后销毁互斥锁和条件变量

           */
        pthread_mutex_lock(&(pool->lock));
        pthread_mutex_destroy(&(pool->lock));
        pthread_cond_destroy(&(pool->notify));
    }
    free(pool);    
    return 0;
}


static void *threadpool_thread(void *threadpool)
{
    threadpool_t *pool = (threadpool_t *)threadpool;//定义pool指向当前线程池
    threadpool_task_t task;

    for(;;) {
        /* Lock must be taken to wait on conditional variable */
        pthread_mutex_lock(&(pool->lock));//获取条件变量锁

        /* Wait on condition variable, check for spurious wakeups.
           When returning from pthread_cond_wait(), we own the lock. */
        while((pool->count == 0) && (!pool->shutdown)) {//如果没有关闭并且任务队列中没有待执行的任务，进入休眠并解锁条件变量
            pthread_cond_wait(&(pool->notify), &(pool->lock));
        }

        //虽然threadpool_add每添加一个新任务就通知threadpool_thread，但threadpool_thread只有队列中没有待执行任务时才会等待

        if((pool->shutdown == immediate_shutdown) ||
           ((pool->shutdown == graceful_shutdown) &&
            (pool->count == 0))) {//如果关闭
            break;
        }

        /* Grab our task 线程池每次新建一个线程时，就执行threadpool_thread，本函数从任务队列的头部选取一个函数执行，然后更新任务队列的头部和
         * 带运行任务的数量
         * */
        task.function = pool->queue[pool->head].function;
        task.argument = pool->queue[pool->head].argument;
        pool->head = (pool->head + 1) % pool->queue_size;
        pool->count -= 1;

        /* Unlock */
        pthread_mutex_unlock(&(pool->lock));//释放条件变量锁

        /* Get to work */
        (*(task.function))(task.argument);//通过函数指针调用具体函数。
        //运行该任务，并结束
    }

    pool->started--;//更新正在运行的线程数

    pthread_mutex_unlock(&(pool->lock));   //？？？？？是否重复解锁
    pthread_exit(NULL);
    return(NULL);
}
