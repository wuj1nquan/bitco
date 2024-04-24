#include "coroutine.h"



pthread_key_t global_sched_key;
static pthread_once_t sched_key_once = PTHREAD_ONCE_INIT;



static void
_save_stack(coroutine *co) {
	char* top = co->sched->stack + co->sched->stack_size; // 协程堆栈的顶部位置 top，即调度器堆栈加上堆栈大小
	char dummy = 0;
	assert(top - &dummy <= CO_MAX_STACKSIZE); // 断言协程的堆栈大小小于等于最大堆栈大小 CO_MAX_STACKSIZE
	if (co->stack_size < top - &dummy) { // 如果协程的堆栈大小小于等于 top - &dummy，则重新分配堆栈内存
		co->stack = realloc(co->stack, top - &dummy);
		assert(co->stack != NULL);
	}
	co->stack_size = top - &dummy; // 更新栈大小
	memcpy(co->stack, &dummy, co->stack_size); //  dummy 和 top 之间的内存复制到新分配的堆栈中
}


static void
_load_stack(coroutine *co) { // 加载协程的堆栈
    // 将之前保存的协程堆栈数据从协程的堆栈中复制回调度器的堆栈中，恢复协程的运行状态
	memcpy(co->sched->stack + co->sched->stack_size - co->stack_size, co->stack, co->stack_size);
}

static void _exec(void *lt) { // 执行协程的真正执行函数
	coroutine *co = (coroutine*)lt; // 接受一个指向协程结构的指针 lt，将其转换为 coroutine 类型
	co->func(co->arg); // 调用协程的执行函数 co->func
	co->status |= (BIT(COROUTINE_STATUS_EXITED) | BIT(COROUTINE_STATUS_FDEOF) | BIT(COROUTINE_STATUS_DETACH)); // 函数执行完毕后标记协程的状态
	coroutine_yield(co); // 将控制权交给调度器
}



void coroutine_free(coroutine *co) { // 释放协程内存资源，移出调度器
	if (co == NULL) return ;
	co->sched->spawned_coroutines --; // 调度器中协程数--

	if (co->stack) {
		free(co->stack); // 释放栈空间
		co->stack = NULL; // 避免重复释放
	}

	free(co); // 释放结构体空间
    co = NULL; // 避免重复释放
}



static void coroutine_init(coroutine *co) { // 初始化一个协程结构体，为协程设置执行上下文、堆栈、执行函数等，并将其状态设置为就绪状态

	getcontext(&co->ctx); // 初始化协程的上下文

    //设置协程部分上下文属性：栈空间、栈大小、链接
	co->ctx.uc_stack.ss_sp = co->sched->stack; 
	co->ctx.uc_stack.ss_size = co->sched->stack_size;
	co->ctx.uc_link = &co->sched->ctx; // 链接到协程所属的调度器的上下文

	makecontext(&co->ctx, (void (*)(void)) _exec, 1, (void*)co); // 将执行函数 _exec 关联到协程的上下文中

	co->status = BIT(COROUTINE_STATUS_READY); // 将协程的状态设置为就绪状态
	
}



void coroutine_yield(coroutine *co) { // 协程让出cpu控制权

	co->ops = 0; // 设置操作码

	if ((co->status & BIT(COROUTINE_STATUS_EXITED)) == 0) { // 通过位与操作检查协程的状态，判断协程是否已经退出

		_save_stack(co); // 保存协程的堆栈。这是因为协程让出执行权时，需要保存当前的堆栈状态，以便再次执行时能恢复执行状态
	}

	swapcontext(&co->ctx, &co->sched->ctx); // 将执行权交还给调度器

}



int coroutine_resume(coroutine *co) { // 恢复一个挂起的协程并开始执行
	
	if (co->status & BIT(COROUTINE_STATUS_NEW)) { // 如果是新创建的协程，先初始化
		coroutine_init(co);
	} 
	
	else { // 协程之前已经被执行过

		_load_stack(co); // 加载堆栈状态
	}

	schedule *sched = coroutine_get_sched(); // 获取当前线程的调度器


	/* 注意！！！
	唯一设置sched->curr_thread的代码*/

	sched->curr_thread = co; // 将调度器中正在运行的协程设置为此协程
	swapcontext(&sched->ctx, &co->ctx); // 将调度器的上下文切换为协程的上下文，开始执行协程
	sched->curr_thread = NULL; // 在切换回调度器的上下文后，将sched->curr_thread 设置为 NULL，表示当前线程没有正在执行的协程



	if (co->status & BIT(COROUTINE_STATUS_EXITED)) { // 表示协程已经退出

		if (co->status & BIT(COROUTINE_STATUS_DETACH)) { // 需要释放资源
			coroutine_free(co);
		}
		return -1; // 返回 -1，表示协程已经退出
	} 

	return 0; // 返回 0，表示协程执行成功
}



void coroutine_renice(coroutine *co) { // 调整协程的优先级，如果协程已经运行了一段时间（操作次数大于等于5），则将其移到就绪队列的尾部，以便后续重新调度

	co->ops ++;
	if (co->ops < 5) return ;

	TAILQ_INSERT_TAIL(&coroutine_get_sched()->ready, co, ready_next); // 将协程插入到就绪队列的尾部
	coroutine_yield(co); 
}



void coroutine_sleep(uint64_t msecs) { // 让当前协程休眠

	coroutine *co = coroutine_get_sched()->curr_thread; // 获取当前调度器，并从调度器中获取当前正在执行的协程指针 co
 
	if (msecs == 0) { // 表示需要让当前协程立即让出执行权并进入就绪状态

		TAILQ_INSERT_TAIL(&co->sched->ready, co, ready_next); // 将当前协程插入到就绪队列的尾部，以待后续调度
		coroutine_yield(co); // 将控制权交给调度器

	} else { // 表示需要将当前协程置于休眠状态
		schedule_sched_sleepdown(co, msecs); // 将当前协程置于休眠状态
	}
}



void coroutine_detach(void) { // 将当前协程标记为 DETACH 状态
	coroutine *co = coroutine_get_sched()->curr_thread;
	co->status |= BIT(COROUTINE_STATUS_DETACH);
}

static void coroutine_sched_key_destructor(void *data) { // 线程局部存储的析构函数，用于在线程退出时释放线程局部存储中的数据
    // 数据 "data" 将会被传递给这个析构函数进行释放
	free(data);
}

static void coroutine_sched_key_creator(void) { // 创建线程局部存储的键

	assert(pthread_key_create(&global_sched_key, coroutine_sched_key_destructor) == 0); // 创建线程局部存储的键 global_sched_key，并指定了析构函数为 nty_coroutine_sched_key_destructor
	assert(pthread_setspecific(global_sched_key, NULL) == 0); // 将线程局部存储的初始值设置为 NULL
	
	return ; // 如果键的创建和设置都成功，则函数返回
}



int coroutine_create(coroutine **new_co, proc_coroutine func, void *arg) { // 创建一个新的协程，并将其添加到调度器的就绪队列

	assert(pthread_once(&sched_key_once, coroutine_sched_key_creator) == 0); // 保证调度器的键只会被创建一次: 确保 coroutine_sched_key_creator 函数只会被执行一次，用于创建线程局部存储的键保证调度器的键只会被创建一次
	schedule *sched = coroutine_get_sched(); // 获取当前线程的调度器

	if (sched == NULL) { // 当前线程尚未拥有调度器，需要先创建调度器:

		schedule_create(0); // 创建调度器 
		
		sched = coroutine_get_sched();
		if (sched == NULL) { // 创建调度器失败
			printf("Failed to create scheduler\n");
			return -1;
		}
	}

	coroutine *co = calloc(1, sizeof(coroutine)); // 为新的协程分配内存空间
	if (co == NULL) { // 分配失败
		printf("Failed to allocate memory for new coroutine\n");
		return -2;
	}

    // 初始化协程各个成员
	co->stack = NULL;
	co->stack_size = 0;

	co->sched = sched; // 所属调度器
	co->status = BIT(COROUTINE_STATUS_NEW); // 状态：新建
	co->id = sched->spawned_coroutines ++; // 协程的id，同时也是调度器中已创建的协程数量
	co->func = func; // 执行的函数

	co->fd_wait = -1;

	co->arg = arg; // 函数参数
	co->birth = coroutine_usec_now(); // 协程创建的时间戳
    
	*new_co = co; // 将新创建的协程指针赋给传入的参数

	TAILQ_INSERT_TAIL(&co->sched->ready, co, ready_next); // 将协程插入到调度器的就绪队列的尾部，以便后续调度器可以从就绪队列中选择协程执行

	return 0;
}
