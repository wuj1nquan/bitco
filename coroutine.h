#define _GNU_SOURCE
#include <dlfcn.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <limits.h>
#include <assert.h>
#include <inttypes.h>
#include <unistd.h>
#include <pthread.h>
#include <fcntl.h>
#include <sys/time.h>
#include <sys/mman.h>
#include <netinet/tcp.h>

#include <ucontext.h>

#include <sys/epoll.h>
#include <sys/poll.h>

#include <errno.h>

#include "queue.h"
#include "tree.h"

// Author : WangBoJing , email : 1989wangbojing@gmail.com
#define CO_MAX_EVENTS		(1024*1024)
#define CO_MAX_STACKSIZE	(128*1024) // {http: 16*1024, tcp: 4*1024}

#define BIT(x)	 				(1 << (x))
#define CLEARBIT(x) 			~(1 << (x))





typedef void (*proc_coroutine)(void *);


typedef enum {
	COROUTINE_STATUS_WAIT_READ,
	COROUTINE_STATUS_WAIT_WRITE,
	COROUTINE_STATUS_NEW,
	COROUTINE_STATUS_READY,
	COROUTINE_STATUS_EXITED,
	COROUTINE_STATUS_BUSY,
	COROUTINE_STATUS_SLEEPING,
	COROUTINE_STATUS_EXPIRED,
	COROUTINE_STATUS_FDEOF,
	COROUTINE_STATUS_DETACH,
	COROUTINE_STATUS_CANCELLED,
	COROUTINE_STATUS_PENDING_RUNCOMPUTE,
	COROUTINE_STATUS_RUNCOMPUTE,
	COROUTINE_STATUS_WAIT_IO_READ,
	COROUTINE_STATUS_WAIT_IO_WRITE,
	COROUTINE_STATUS_WAIT_MULTI
} coroutine_status;

typedef enum {
	COROUTINE_COMPUTE_BUSY,
	COROUTINE_COMPUTE_FREE
} coroutine_compute_status;

typedef enum {
	COROUTINE_EV_READ,
	COROUTINE_EV_WRITE
} coroutine_event;





/*
#define	LIST_HEAD(name, type)							\
	struct name {										\
		struct type *lh_first; // first element	        \      
	}
*/
LIST_HEAD(_coroutine_link, _coroutine);


/*
#define	TAILQ_HEAD(name, type)										\
	struct name {													\
		struct type *tqh_first; // first element  				    \
		struct type **tqh_last; // addr of last next element  	    \
		TRACEBUF													\
	}
*/
TAILQ_HEAD(_coroutine_queue, _coroutine);


/*
#define RB_HEAD(name, type)                             \
	struct name {                                       \
		struct type *rbh_root; //  root of the tree     \
	}
*/
RB_HEAD(_coroutine_rbtree_sleep, _coroutine);
RB_HEAD(_coroutine_rbtree_wait, _coroutine);


typedef struct _coroutine_link coroutine_link;
typedef struct _coroutine_queue coroutine_queue;

typedef struct _coroutine_rbtree_sleep coroutine_rbtree_sleep;
typedef struct _coroutine_rbtree_wait coroutine_rbtree_wait;






typedef struct schedule { // 调度器

	uint64_t birth;  // 创建时间戳

	ucontext_t ctx; // 调度器上下文

	void *stack; // 栈空间
	size_t stack_size; // 栈大小
	int spawned_coroutines; // 已创建的协程数量
	uint64_t default_timeout; // 默认超时时间
	struct _coroutine *curr_thread; // 当前正在执行的协程，只在 coroutine_resume 函数中设置
	int page_size; //页大小

	int poller_fd; // 由epoll_craete创建的epoll实例
	int eventfd; // 用于事件通知的文件描述符
	struct epoll_event eventlist[CO_MAX_EVENTS]; // 存储 epoll_wait 函数返回的就绪事件
	int nevents; 

	int num_new_events; // poller_fd中就绪的事件数量
	pthread_mutex_t defer_mutex; // 延迟队列的互斥锁

	coroutine_queue ready; // 就绪队列
	coroutine_queue defer; // 延迟队列

	coroutine_link busy; // 忙碌链表
	
	coroutine_rbtree_sleep sleeping; // 睡眠红黑树
	coroutine_rbtree_wait waiting; // 等待红黑树


} schedule;




typedef struct _coroutine {

	ucontext_t ctx; // 协程的上下文信息

	proc_coroutine func; // 协程执行的函数
	void *arg; // 传递给协程执行函数的参数
	void *data; // 协程私有数据
	size_t stack_size; // 栈大小
	size_t last_stack_size; // 上次栈大小
	
	coroutine_status status; // 协程的状态
	schedule *sched; // 指向协程所属的调度器

	uint64_t birth; // 协程创建的事件戳
	uint64_t id; // 协程的唯一标识符

	int fd; // 与协程关联的文件描述符
	unsigned short events;  //POLL_EVENT

	char funcname[64]; //协程执行的函数名称
	struct _coroutine *co_join; 

	void **co_exit_ptr; 
	void *stack; // 栈空间
	void *ebp; //栈指针
	uint32_t ops; // 当前协程的操作码
	uint64_t sleep_usecs; //休眠时间

	RB_ENTRY(_coroutine) sleep_node; // 睡眠队列中的红黑树节点
	RB_ENTRY(_coroutine) wait_node; // 等待队列中的红黑树节点

	LIST_ENTRY(_coroutine) busy_next; // 忙碌协程链表中的下一个指针

	TAILQ_ENTRY(_coroutine) ready_next; // 就绪队列中的下一个指针
	TAILQ_ENTRY(_coroutine) defer_next; // 延迟队列中的下一个指针
	TAILQ_ENTRY(_coroutine) cond_next; // 条件变量队列中的下一个指针

	TAILQ_ENTRY(_coroutine) io_next; //  I/O 就绪队列中的下一个指针
	TAILQ_ENTRY(_coroutine) compute_next; // 计算就绪队列中的下一个指针

	struct { // 协程执行 I/O 操作所需的相关信息
		void *buf;
		size_t nbytes;
		int fd;
		int ret;
		int err;
	} io;

	struct coroutine_compute_sched *compute_sched; // 指向计算调度器的指针
	int ready_fds; // 就绪的文件描述符数量
	struct pollfd *pfds; // 指向 pollfd 结构体数组的指针
	nfds_t nfds; // pollfd 数组的大小

} coroutine;




typedef struct coroutine_compute_sched {

	ucontext_t ctx; // 上下文信息

	coroutine_queue coroutines; // 协程队列
 
 	coroutine *curr_coroutine;  // 当前协程指针

	pthread_mutex_t run_mutex; 
	pthread_cond_t run_cond;

	pthread_mutex_t co_mutex;
	LIST_ENTRY(coroutine_compute_sched) compute_next;  //计算调度队列
	
	coroutine_compute_status compute_status; // 计算状态

} coroutine_compute_sched;





extern pthread_key_t global_sched_key; // extern from "<pthread.h>"

static inline schedule *coroutine_get_sched(void) {
	return pthread_getspecific(global_sched_key); // 获取线程局部存储中的调度器指针
}

static inline uint64_t coroutine_diff_usecs(uint64_t t1, uint64_t t2) {
	return t2-t1;
}

static inline uint64_t coroutine_usec_now(void) {
	struct timeval t1 = {0, 0};
	gettimeofday(&t1, NULL);

	return t1.tv_sec * 1000000 + t1.tv_usec;
}





int epoller_create(void);


void schedule_cancel_event(coroutine *co);
void schedule_sched_event(coroutine *co, int fd, coroutine_event e, uint64_t timeout);

void schedule_desched_sleepdown(coroutine *co);
void schedule_sched_sleepdown(coroutine *co, uint64_t msecs);

coroutine* schedule_desched_wait(int fd);
void schedule_sched_wait(coroutine *co, int fd, unsigned short events, uint64_t timeout);

void schedule_run(void);

int epoller_ev_register_trigger(void);
int epoller_wait(struct timespec t);
int coroutine_resume(coroutine *co);
void coroutine_free(coroutine *co);
int coroutine_create(coroutine **new_co, proc_coroutine func, void *arg);
void coroutine_yield(coroutine *co);

void coroutine_sleep(uint64_t msecs);



