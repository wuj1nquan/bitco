#include "coroutine.h"



// Author : WangBoJing , email : 1989wangbojing@gmail.com
#define FD_KEY(f,e) (((int64_t)(f) << (sizeof(int32_t) * 8)) | e)
#define FD_EVENT(f) ((int32_t)(f))
#define FD_ONLY(f) ((f) >> ((sizeof(int32_t) * 8)))




/*
这两个比较函数的目的是在红黑树中对协程按照睡眠时间或者等待事件描述符进行排序，以便在调度器中根据不同的条件进行高效的协程调度
*/
// 如果 co1 的睡眠时间小于 co2 的睡眠时间，则返回 -1；如果二者相等，则返回 0；否则返回 1。
static inline int coroutine_sleep_cmp(coroutine *co1, coroutine *co2) {
	if (co1->sleep_usecs < co2->sleep_usecs) {
		return -1;
	}
	if (co1->sleep_usecs == co2->sleep_usecs) {
		return 0;
	}
	return 1;
}


//如果 co1 的 fd_wait 小于 co2 的 fd_wait，则返回 -1；如果二者相等，则返回 0；否则返回 1。
static inline int coroutine_wait_cmp(coroutine *co1, coroutine *co2) {

	if (co1->fd_wait < co2->fd_wait) {
		return -1;
	}
	if (co1->fd_wait == co2->fd_wait) {
		return 0;
	}

	return 1;
}



/* #define	RB_GENERATE(name, type, field, cmp)									\
        RB_GENERATE_INTERNAL(name, type, field, cmp,)
*/
RB_GENERATE(_coroutine_rbtree_sleep, _coroutine, sleep_node, coroutine_sleep_cmp);
RB_GENERATE(_coroutine_rbtree_wait, _coroutine, wait_node, coroutine_wait_cmp);



// 使协程进入睡眠，并设置红黑树节点
void schedule_sched_sleepdown(coroutine *co, uint64_t msecs) { // 参数 co 是需要进入睡眠状态的协程指针，msecs 是协程需要睡眠的时间

	uint64_t usecs = msecs * 1000u; // 将 msecs 转换为微秒

    // 在睡眠红黑树中查找是否已经存在该协程。如果已经存在，则先移除该协程
	coroutine *co_tmp = RB_FIND(_coroutine_rbtree_sleep, &co->sched->sleeping, co);
	if (co_tmp != NULL) {
		RB_REMOVE(_coroutine_rbtree_sleep, &co->sched->sleeping, co_tmp);
	}
    
	co->sleep_usecs = coroutine_diff_usecs(co->sched->birth, coroutine_usec_now()) + usecs; // 计算出协程需要睡眠的总时长

	while (msecs) {

		co_tmp = RB_INSERT(_coroutine_rbtree_sleep, &co->sched->sleeping, co);

		if (co_tmp) { // 如果添加失败，说明在睡眠红黑树中已经存在相同睡眠时长的协程，此时会对睡眠时长进行自增，直到找到一个合适的睡眠时长为止
			printf("1111 sleep_usecs %"PRIu64"\n", co->sleep_usecs);
			co->sleep_usecs ++;
			continue;
		}
		co->status |= BIT(COROUTINE_STATUS_SLEEPING); // 将协程的状态设置为睡眠状态
		break;
	}

	//yield
}


// 将协程移出睡眠红黑树
void schedule_desched_sleepdown(coroutine *co) {
	if (co->status & BIT(COROUTINE_STATUS_SLEEPING)) { // 如果处于睡眠状态，则从睡眠红黑树中移除该协程
		RB_REMOVE(_coroutine_rbtree_sleep, &co->sched->sleeping, co);

		co->status &= CLEARBIT(COROUTINE_STATUS_SLEEPING); // 清除睡眠状态
		co->status |= BIT(COROUTINE_STATUS_READY); // 设置为就绪状态
		co->status &= CLEARBIT(COROUTINE_STATUS_EXPIRED); // 清除过期状态
	}
}


// 在等待红黑树中查找指定协程
coroutine *schedule_search_wait(int fd) {
	coroutine find_it = {0};
	find_it.fd = fd;

	schedule *sched = coroutine_get_sched();

	// 在等待红黑树中查找具有指定文件描述符的协程对象。如果找到了匹配的协程，则将其状态清零并返回该协程指针, 未找到返回NULL 
	coroutine *co = RB_FIND(_coroutine_rbtree_wait, &sched->waiting, &find_it);
	co->status = 0;

	return co;
}


// 在等待红黑树中移除一个协程
coroutine* schedule_desched_wait(int fd) {
	
	coroutine find_it = {0};
	find_it.fd = fd;

	schedule *sched = coroutine_get_sched();
	
	coroutine *co = RB_FIND(_coroutine_rbtree_wait, &sched->waiting, &find_it);
	if (co != NULL) { // 如果找到了匹配的协程，则从等待红黑树中移除该协程
		RB_REMOVE(_coroutine_rbtree_wait, &co->sched->waiting, co);
	}
    // 将协程的状态清零，并调用 schedule_desched_sleepdown 函数将其从睡眠状态中移除，最后返回该协程指针, 未找到返回NULL
	co->status = 0;
	schedule_desched_sleepdown(co);
	
	return co;
}


/* 将协程设置为等待状态，等待指定文件描述符上的事件，并将其加入到等待红黑树中 */
void schedule_sched_wait(coroutine *co, int fd, unsigned short events, uint64_t timeout) { // timeout为 1 则不设置为睡眠状态
	// 检查协程的当前状态，如果协程已经处于等待读或写事件的状态，则输出错误信息并终止程序。
    // 这个检查确保了协程在设置等待状态之前不会处于其他等待状态
	if (co->status & BIT(COROUTINE_STATUS_WAIT_READ) ||
		co->status & BIT(COROUTINE_STATUS_WAIT_WRITE)) {
		printf("Unexpected event. lt id %"PRIu64" fd %"PRId32" already in %"PRId32" state\n",
            co->id, co->fd, co->status);
		assert(0);
	}

    // 根据参数 events 中的事件类型（POLLIN 或 POLLOUT），设置协程的状态为等待读或等待写状态
	if (events & POLLIN) { 
		co->status |= COROUTINE_STATUS_WAIT_READ;
	} else if (events & POLLOUT) {
		co->status |= COROUTINE_STATUS_WAIT_WRITE;
	} else {
		printf("events : %d\n", events);
		assert(0);
	}

	co->fd = fd; // 表示协程要等待的文件描述符
	co->events = events; // 表示协程要等待的事件类型

    // 将协程插入到等待红黑树中。如果红黑树中已经存在具有相同键值的节点，则会返回非NULL，表示插入失败
	coroutine *co_tmp = RB_INSERT(_coroutine_rbtree_wait, &co->sched->waiting, co);

	assert(co_tmp == NULL);

	//检查参数 timeout 是否为 1。如果是，直接返回。否则，设置协程为睡眠状态
	if (timeout == 1) return ; //Error

	schedule_sched_sleepdown(co, timeout);
	
}


// 取消协程的等待状态，并将其从等待红黑树中移除
void schedule_cancel_wait(coroutine *co) {
	RB_REMOVE(_coroutine_rbtree_wait, &co->sched->waiting, co);
}


// 释放调度器的内存资源
void schedule_free(schedule *sched) {
	if (sched->poller_fd > 0) {
		close(sched->poller_fd); // 释放epoll实例
	}
	if (sched->eventfd > 0) {
		close(sched->eventfd); // 释放eventfd实例
	}
	if (sched->stack != NULL) {
		free(sched->stack); // 释放栈空间
	}
	
	free(sched); // 释放结构体

    // 将线程特定数据 global_sched_key 关联的值设置为 NULL，以确保不再使用已释放的调度器
	assert(pthread_setspecific(global_sched_key, NULL) == 0);
}



// 创建并初始化一个调度器
int schedule_create(int stack_size) {

	int sched_stack_size = stack_size ? stack_size : CO_MAX_STACKSIZE;

	schedule *sched = (schedule*)calloc(1, sizeof(schedule));
	if (sched == NULL) {
		printf("Failed to initialize scheduler\n");
		return -1;
	}

    // 将线程特定数据 global_sched_key 关联的值设置为新创建的调度器结构体，以便其他函数可以通过该键值获取到当前线程的调度器
	assert(pthread_setspecific(global_sched_key, sched) == 0);

	sched->poller_fd = epoller_create(); // 使用epoll_create创建一个epoll实例
	if (sched->poller_fd == -1) {
		printf("Failed to initialize epoller\n");
		schedule_free(sched);
		return -2;
	}


/* 调用eventfd函数： sched->eventfd = eventfd(0, EFD_NONBLOCK);
   并将其加入到调度器的epoll管理中： epoll_ctl(sched->poller_fd, EPOLL_CTL_ADD, sched->eventfd, &ev); */
	epoller_ev_register_trigger(); 
                                   

	sched->stack_size = sched_stack_size;
	sched->page_size = getpagesize();

	int ret = posix_memalign(&sched->stack, sched->page_size, sched->stack_size); 
    // 使用 posix_memalign 函数分配调度器的栈空间，并确保分配的内存按页对齐。如果分配失败，则终止程序
	assert(ret == 0);

	sched->spawned_coroutines = 0; // 已创建协程数量
	sched->default_timeout = 3000000u; // 默认超时时间

    // 初始化睡眠红黑树和等待红黑树
	RB_INIT(&sched->sleeping);
	RB_INIT(&sched->waiting);

    // 记录调度器的创建时间
	sched->birth = coroutine_usec_now();

    // 初始化调度器的就绪队列、延迟队列和忙碌链表
	TAILQ_INIT(&sched->ready);
	TAILQ_INIT(&sched->defer);
	LIST_INIT(&sched->busy);


    return 0;
}


// 检查并返回最先超时的协程
static coroutine *schedule_expired(schedule *sched) {
	
	uint64_t t_diff_usecs = coroutine_diff_usecs(sched->birth, coroutine_usec_now()); // 计算当前时间与调度器创建时间之间的时间差
	coroutine *co = RB_MIN(_coroutine_rbtree_sleep, &sched->sleeping); // 获取睡眠红黑树中的最小值，即最先超时的协程
	if (co == NULL) return NULL;
	
    // 如果找到了最小超时的协程，则检查其超时时间是否小于等于当前时间与调度器创建时间之间的时间差。
    // 如果是，则从睡眠红黑树中移除该协程（RB_REMOVE），并将其返回
	if (co->sleep_usecs <= t_diff_usecs) { 
		RB_REMOVE(_coroutine_rbtree_sleep, &co->sched->sleeping, co);
		return co;
	}
	return NULL;
}



// 检查调度器是否已经完成了所有的任务，即等待队列、忙碌链表、睡眠红黑树和就绪队列是否都为空
static inline int schedule_isdone(schedule *sched) {
	return (RB_EMPTY(&sched->waiting) && 
		LIST_EMPTY(&sched->busy) &&
		RB_EMPTY(&sched->sleeping) &&
		TAILQ_EMPTY(&sched->ready));
}



// 获取调度器中最小的超时时间，即睡眠红黑树中最小的睡眠时间与当前时间之差
static uint64_t schedule_min_timeout(schedule *sched) {
	uint64_t t_diff_usecs = coroutine_diff_usecs(sched->birth, coroutine_usec_now()); // 计算调度器从创建到现在的时间差，并将结果保存在变量 t_diff_usecs 中
	uint64_t min = sched->default_timeout; // 将min初始化为调度器默认超时时间

	coroutine *co = RB_MIN(_coroutine_rbtree_sleep, &sched->sleeping); // 使用 RB_MIN 宏获取睡眠红黑树中最小的协程对象，即具有最小睡眠时间的协程
	if (!co) return min; // 如果睡眠红黑树为空或者最小睡眠时间大于当前时间差，则返回默认超时时间

    // 否则，返回最小睡眠时间与当前时间差之间的差值，即表示还需要等待的时间
	min = co->sleep_usecs;
	if (min > t_diff_usecs) {
		return min - t_diff_usecs;
	}

	return 0;
} 


// 轮询调度器中epoll管理的事件：epoll_wait(sched->poller_fd, sched->eventlist
static int schedule_epoll(schedule *sched) {

	sched->num_new_events = 0;

	struct timespec t = {0, 0};
	uint64_t usecs = schedule_min_timeout(sched);
	if (usecs && TAILQ_EMPTY(&sched->ready)) {
		t.tv_sec = usecs / 1000000u;
		if (t.tv_sec != 0) {
			t.tv_nsec = (usecs % 1000u) * 1000u;
		} else {
			t.tv_nsec = usecs * 1000u;
		}
	} else {
		return 0;
	}

	int nready = 0;
	while (1) {
		nready = epoller_wait(t);
		if (nready == -1) {
			if (errno == EINTR) continue;
			else assert(0);
		}
		break;
	}

	sched->nevents = 0;
	sched->num_new_events = nready; // 就绪事件数量

	return 0;
}



// 调度器主循环 实现了调度器的主要逻辑，包括处理超时、处理就绪队列、等待事件并处理事件等操作
void schedule_run(void) {

	schedule *sched = coroutine_get_sched(); // 获取当前调度器
	if (sched == NULL) return ;

	while (!schedule_isdone(sched)) {
		// 1. expried coroutine in sleep rbtree
		// 获取超时的协程，并逐个执行这些协程的恢复操作
		coroutine *expired = NULL;
		while ((expired = schedule_expired(sched)) != NULL) {
			coroutine_resume(expired);
		}
		// 2. ready queue
        // 处理就绪队列中的协程：从就绪队列中依次取出协程，并执行它们的恢复操作
		coroutine *last_co_ready = TAILQ_LAST(&sched->ready, _coroutine_queue);
		while (!TAILQ_EMPTY(&sched->ready)) {
			coroutine *co = TAILQ_FIRST(&sched->ready);
			TAILQ_REMOVE(&co->sched->ready, co, ready_next);
            
			if (co->status & BIT(COROUTINE_STATUS_FDEOF)) { // 表示文件描述符已关闭，这时释放该协程的资源，然后退出循环。
				coroutine_free(co);
				break;
			}
    
			coroutine_resume(co);
			if (co == last_co_ready) break; // 全部处理完即退出
		}

		// 3. wait rbtree
		schedule_epoll(sched); // 调用epoll_wait轮询调度器中epoll管理的文件描述符，并将就绪事件保存到调度器的 eventlist 中

		while (sched->num_new_events) { // 遍历所有就绪事件

            // 从 num_new_events 中获取事件数量，并将事件数量减一
			int idx = --sched->num_new_events;
            // 获取对应索引处的事件
			struct epoll_event *ev = sched->eventlist+idx;
			
			int fd = ev->data.fd;
            // 检查事件是否为对端关闭连接
			int is_eof = ev->events & EPOLLHUP;
            // 如果事件为对端关闭连接，则设置 errno 为 ECONNRESET
			if (is_eof) errno = ECONNRESET;

            // 根据文件描述符在等待红黑树中查找对应的协程
			coroutine *co = schedule_search_wait(fd);

            // 如果找到了对应的协程，则恢复该协程的执行
			if (co != NULL) {
				if (is_eof) { // 如果事件为对端关闭连接，则设置协程状态为已关闭文件描述符
					co->status |= BIT(COROUTINE_STATUS_FDEOF);
				}
                // 恢复协程的执行
				coroutine_resume(co);
			}
            // 将 is_eof 重置为 0，以便下次循环使用
			is_eof = 0;
		}
	}

	schedule_free(sched); // 所有任务都完成后,释放调度器的资源，并返回。
	
	return ;
}




