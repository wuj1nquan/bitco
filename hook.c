#include "coroutine.h"



/* 获取系统调用签名 */
typedef int (*socket_t)(int domain, int type, int protocol);
typedef int(*connect_t)(int sockfd, const struct sockaddr *addr, socklen_t addrlen);

typedef ssize_t(*read_t)(int, void *, size_t);
typedef ssize_t(*recv_t)(int sockfd, void *buf, size_t len, int flags);

typedef ssize_t(*recvfrom_t)(int sockfd, void *buf, size_t len, int flags,
                            struct sockaddr *src_addr, socklen_t *addrlen);
typedef ssize_t(*write_t)(int, const void *, size_t);

typedef ssize_t(*send_t)(int sockfd, const void *buf, size_t len, int flags);
typedef ssize_t(*sendto_t)(int sockfd, const void *buf, size_t len, int flags,
                          const struct sockaddr *dest_addr, socklen_t addrlen);

typedef int(*accept_t)(int sockfd, struct sockaddr *addr, socklen_t *addrlen);
typedef int(*close_t)(int fd);

/* 真正的系统调用 */
socket_t socket_f = (socket_t)dlsym(RTLD_NEXT, "socket");
connect_t connect_f = (connect_t)dlsym(RTLD_NEXT, "connect");

read_t read_f = (read_t)dlsym(RTLD_NEXT, "read");
recv_t recv_f = (recv_t)dlsym(RTLD_NEXT, "recv");

recvfrom_t recvfrom_f = (recvfrom_t)dlsym(RTLD_NEXT, "recvfrom");
write_t write_f = (write_t)dlsym(RTLD_NEXT, "write");

send_t send_f = (send_t)dlsym(RTLD_NEXT, "send");
sendto_t sendto_f = (sendto_t)dlsym(RTLD_NEXT, "sendto");

accept_t accept_f = (accept_t)dlsym(RTLD_NEXT, "accept");
close_t close_f = (close_t)dlsym(RTLD_NEXT, "close");




// 将poll事件转换成epoll事件
static uint32_t pollevent_2epoll( short events )
{
	uint32_t e = 0;	
	if( events & POLLIN ) 	e |= EPOLLIN;
	if( events & POLLOUT )  e |= EPOLLOUT;
	if( events & POLLHUP ) 	e |= EPOLLHUP;
	if( events & POLLERR )	e |= EPOLLERR;
	if( events & POLLRDNORM ) e |= EPOLLRDNORM;
	if( events & POLLWRNORM ) e |= EPOLLWRNORM;
	return e;
}

// 将epoll事件转换成poll事件
static short epollevent_2poll( uint32_t events )
{
	short e = 0;	
	if( events & EPOLLIN ) 	e |= POLLIN;
	if( events & EPOLLOUT ) e |= POLLOUT;
	if( events & EPOLLHUP ) e |= POLLHUP;
	if( events & EPOLLERR ) e |= POLLERR;
	if( events & EPOLLRDNORM ) e |= POLLRDNORM;
	if( events & EPOLLWRNORM ) e |= POLLWRNORM;
	return e;
}


/* 封装poll，对调度器进行耦合 */

static int poll_inner(struct pollfd *fds, nfds_t nfds, int timeout) {

	if (timeout == 0)
	{
		return poll(fds, nfds, timeout);
	}
	if (timeout < 0)
	{
		timeout = INT_MAX;
	}

	schedule *sched = coroutine_get_sched(); // 获取当前线程的调度器
	if (sched == NULL) {
		printf("scheduler not exit!\n");
		return -1;
	}
	
	coroutine *co = sched->curr_thread; // 获取当前正在执行的协程，也就是调用poll_inner所在函数（例如read）所在的协程
	
	int i = 0;
	for (i = 0;i < nfds;i ++) { // 将poll监视的文件描述符和相应的事件转换为 epoll 事件
	
		struct epoll_event ev;
		ev.events = pollevent_2epoll(fds[i].events);
		ev.data.fd = fds[i].fd;
		epoll_ctl(sched->poller_fd, EPOLL_CTL_ADD, fds[i].fd, &ev);

		co->events = fds[i].events;
		schedule_sched_wait(co, fds[i].fd, fds[i].events, timeout); // 让当前协程等待事件
	}
	coroutine_yield(co); // 让当前协程放弃 CPU 控制权，等待事件发生

	for (i = 0;i < nfds;i ++) { // 处理事件完成后从 epoll 实例中删除已经监视的文件描述符，并清除相应的等待状态
	
		struct epoll_event ev;
		ev.events = pollevent_2epoll(fds[i].events);
		ev.data.fd = fds[i].fd;
		epoll_ctl(sched->poller_fd, EPOLL_CTL_DEL, fds[i].fd, &ev);

		schedule_desched_wait(fds[i].fd); // 处理完成后将该协程从调度器的等待红黑树中删除
	}

	return nfds; // 返回监视文件描述符数量
}






/* 覆盖原系统调用 */


int socket(int domain, int type, int protocol) {

	int fd = socket_f(domain, type, protocol);
	if (fd == -1) {
		printf("Failed to create a new socket\n");
		return -1;
	}
	int ret = fcntl(fd, F_SETFL, O_NONBLOCK); // 默认将所有套接字文件描述符设置为非阻塞模式
	if (ret == -1) {
		close(ret);
		return -1;
	}
	int reuse = 1;
	setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, (char *)&reuse, sizeof(reuse)); // 设置套接字选项 SO_REUSEADDR，允许在套接字关闭后立即重用之前使用的地址
	
	return fd;
}


ssize_t read(int fd, void *buf, size_t count) { // 上下文切换实现非阻塞read

	struct pollfd fds;
	fds.fd = fd;
	fds.events = POLLIN | POLLERR | POLLHUP;

	poll_inner(&fds, 1, 1); // 将当前fd交由epoll管理并让出cpu，避免read_f阻塞，等待fd就绪后由调度器返回到这里继续执行read_f

	int ret = read_f(fd, buf, count);
	if (ret < 0) {
		
		if (errno == ECONNRESET) return -1;
	}

	return ret;
}



ssize_t recv(int fd, void *buf, size_t len, int flags) { // 上下文切换实现非阻塞recv

	struct pollfd fds;
	fds.fd = fd;
	fds.events = POLLIN | POLLERR | POLLHUP;

	poll_inner(&fds, 1, 1);  // 将当前fd交由epoll管理并让出cpu，避免阻塞，等待fd就绪后由调度器返回到这里继续执行

	int ret = recv_f(fd, buf, len, flags);
	if (ret < 0) {

		if (errno == ECONNRESET) return -1;
		
	}

	return ret;
}



ssize_t recvfrom(int fd, void *buf, size_t len, int flags,
                struct sockaddr *src_addr, socklen_t *addrlen) {

	struct pollfd fds;
	fds.fd = fd;
	fds.events = POLLIN | POLLERR | POLLHUP;

	poll_inner(&fds, 1, 1); // 将当前fd交由epoll管理并让出cpu，避免阻塞，等待fd就绪后由调度器返回到这里继续执行

	int ret = recvfrom_f(fd, buf, len, flags, src_addr, addrlen);
	if (ret < 0) {
		if (errno == EAGAIN) return ret;
		if (errno == ECONNRESET) return 0;
		
		printf("recv error : %d, ret : %d\n", errno, ret);
		assert(0);
	}

	return ret;
}



ssize_t write(int fd, const void *buf, size_t count) {

	int sent = 0; // 已写入字节数

	int ret = write_f(fd, ((char*)buf)+sent, count-sent); // 先进行一次写入，不判断fd是否可写，未阻塞
	if (ret == 0) return ret;
	if (ret > 0) sent += ret;

	while (sent < count) { // 如果第一次没有写完，等到fd可写再写入，避免阻塞
		struct pollfd fds;
		fds.fd = fd;
		fds.events = POLLOUT | POLLERR | POLLHUP;

		poll_inner(&fds, 1, 1); // 将当前fd交由epoll管理并让出cpu，避免阻塞，等待fd就绪后由调度器返回到这里继续执行
		ret = write_f(fd, ((char*)buf)+sent, count-sent);
		if (ret <= 0) {			
			break;
		}
		sent += ret;
	}

	if (ret <= 0 && sent == 0) return ret; // 写入失败
	
	return sent;
}



ssize_t send(int fd, const void *buf, size_t len, int flags) {

	int sent = 0; // 已发送字节数

	int ret = send_f(fd, ((char*)buf)+sent, len-sent, flags); // 先进行一次发送，不判断fd是否可写，未阻塞
	if (ret == 0) return ret;
	if (ret > 0) sent += ret;

	while (sent < len) { // 如果第一次没有发送完，等到fd可写再发送，避免阻塞
		struct pollfd fds;
		fds.fd = fd;
		fds.events = POLLOUT | POLLERR | POLLHUP;

		poll_inner(&fds, 1, 1); // 将当前fd交由epoll管理并让出cpu，避免阻塞，等待fd就绪后由调度器返回到这里继续执行
		ret = send_f(fd, ((char*)buf)+sent, len-sent, flags);
		
		if (ret <= 0) {			
			break;
		}
		sent += ret;
	}

	if (ret <= 0 && sent == 0) return ret; // send失败
	
	return sent;
}



ssize_t sendto(int sockfd, const void *buf, size_t len, int flags,
            const struct sockaddr *dest_addr, socklen_t addrlen) {

	struct pollfd fds;
	fds.fd = sockfd;
	fds.events = POLLOUT | POLLERR | POLLHUP;

	poll_inner(&fds, 1, 1); // 将当前fd交由epoll管理并让出cpu，避免阻塞，等待fd就绪后由调度器返回到这里继续执行

	int ret = sendto_f(sockfd, buf, len, flags, dest_addr, addrlen);
	if (ret < 0) {
		if (errno == EAGAIN) return ret;
		if (errno == ECONNRESET) return 0;
		
		printf("recv error : %d, ret : %d\n", errno, ret);
		assert(0);
	}

	return ret;
}



int accept(int fd, struct sockaddr *addr, socklen_t *len) {

	int sockfd = -1; // 初始化
	int timeout = 1; // 超时时间
	coroutine *co = coroutine_get_sched()->curr_thread; // 获取当前协程
	
	while (1) { // 轮询接受连接
		struct pollfd fds;
		fds.fd = fd;
		fds.events = POLLIN | POLLERR | POLLHUP;
		poll_inner(&fds, 1, timeout); // 将当前fd交由epoll管理并让出cpu，避免阻塞，等待fd就绪后由调度器返回到这里继续执行

		sockfd = accept_f(fd, addr, len);
		if (sockfd < 0) {
			if (errno == EAGAIN) {
				continue;
			} else if (errno == ECONNABORTED) {
				printf("accept : ECONNABORTED\n");
				
			} else if (errno == EMFILE || errno == ENFILE) {
				printf("accept : EMFILE || ENFILE\n");
			}
			return -1;
		} else {
			break;
		}
	}

	int ret = fcntl(sockfd, F_SETFL, O_NONBLOCK); // 默认设置套接字描述符为非阻塞模式
	if (ret == -1) {
		close(sockfd);
		return -1;
	}
	int reuse = 1;
	setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, (char *)&reuse, sizeof(reuse)); // 设置套接字选项 SO_REUSEADDR，允许在套接字关闭后立即重用之前使用的地址
	
	return sockfd;
}



int connect(int fd, const struct sockaddr *addr, socklen_t addrlen) {

	int ret = 0;

	while (1) {

		struct pollfd fds;
		fds.fd = fd;
		fds.events = POLLOUT | POLLERR | POLLHUP;
		poll_inner(&fds, 1, 1);  // 加入epoll管理，让出cpu

		ret = connect_f(fd, addr, addrlen);
		if (ret == 0) break;

		if (ret == -1 && (errno == EAGAIN ||
			errno == EWOULDBLOCK || 
			errno == EINPROGRESS)) {
			continue;
		} else {
			break;
		}
	}

	return ret;
}



int close(int fd) {

	/* 暂时未作修改 */

	return close_f(fd);
}














