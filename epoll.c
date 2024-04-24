#include <sys/eventfd.h>

#include "coroutine.h"



int epoller_create(void) { // 创建一个 epoll 实例，并返回相应的文件描述符
	return epoll_create(1024);
} 

int epoller_wait(struct timespec t) { // 等待事件发生，并返回发生的事件数量  *参数t没有用到

	schedule *sched = coroutine_get_sched(); // 获取当前调度器的指针，并从中获取 epoll 文件描述符 sched->poller_fd

    // 调用 epoll_wait 函数等待事件发生，其中 sched->eventlist 存储了事件列表
    // CO_MAX_EVENTS 是事件列表的最大长度，t.tv_sec*1000.0 + t.tv_nsec/1000000.0 是超时时间
	return epoll_wait(sched->poller_fd, sched->eventlist, CO_MAX_EVENTS, t.tv_sec*1000.0 + t.tv_nsec/1000000.0);
}

int epoller_ev_register_trigger(void) { // 为调度器注册一个通知事件，以便在事件发生时通知 epoll

	schedule *sched = coroutine_get_sched(); // 获取当前调度器

	if (!sched->eventfd) {
		sched->eventfd = eventfd(0, EFD_NONBLOCK); // 创建一个 eventfd 非阻塞文件描述符
		assert(sched->eventfd != -1);
	}


    // 将 eventfd 文件描述符加入到 epoll 实例中
	struct epoll_event ev;
	ev.events = EPOLLIN;
	ev.data.fd = sched->eventfd;
	int ret = epoll_ctl(sched->poller_fd, EPOLL_CTL_ADD, sched->eventfd, &ev);

	assert(ret != -1);
}


/*
*
* eventfd 是一个 Linux 特有的系统调用，用于创建一个用于事件通知的文件描述符。它通常用于在进程之间或者同一个进程内的线程之间进行事件通知。
*
* 其原型如下：
*
* #include <sys/eventfd.h>
*
* int eventfd(unsigned int initval, int flags);
*
* initval 参数是一个无符号整数，指定了初始的计数器值，通常用于指定事件计数的初始值。
* flags 参数指定了事件的行为，可以是 EFD_SEMAPHORE 或者 EFD_CLOEXEC 之一，分别用于指定信号量模式和关闭执行时关闭标志。
* 
* eventfd 创建的文件描述符是一个可以被读写的文件描述符，可以通过 read 和 write 系统调用对其进行操作。
* 主要用途是在多线程或多进程之间进行事件通知，通常配合使用 epoll 或者 select 等 I/O 多路复用机制来监控事件的发生
*
*/