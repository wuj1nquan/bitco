/*
 *  Author : WangBoJing , email : 1989wangbojing@gmail.com
 *
 */
 


#include "coroutine.h"

#include <arpa/inet.h>

#define MAX_CLIENT_NUM			1000000 // 最大客户端连接数
#define TIME_SUB_MS(tv1, tv2)  ((tv1.tv_sec - tv2.tv_sec) * 1000 + (tv1.tv_usec - tv2.tv_usec) / 1000)


void server_reader(void *arg) { // 参数是服务端与客户端通信的套接字clientfd
	int fd = *(int *)arg;
	int ret = 0;

    // 这三行好像没用到
	struct pollfd fds;
	fds.fd = fd;
	fds.events = POLLIN;

	while (1) { // 循环接收来自客户端的消息并回复
		
		char buf[1024] = {0};
		ret = recv(fd, buf, 1024, 0);
		if (ret > 0) {
			if(fd > MAX_CLIENT_NUM) 
			printf("read from server: %.*s\n", ret, buf);
			
			ret = send(fd, buf, strlen(buf), 0);
			if (ret == -1) {
				close(fd);
				break;
			}
		} else if (ret == 0) {	// 对方断开连接，关闭本端套接字
			close(fd);
			break;
		}

	}
}


void server(void *arg) {  // 传入端口号，创建一个服务端监听套接字，并接受循环连接，为每个连接创建一个协程进行通信

	unsigned short port = *(unsigned short *)arg;
	free(arg);

	int fd = socket(AF_INET, SOCK_STREAM, 0);
	if (fd < 0) return ;

	struct sockaddr_in local, remote;
	local.sin_family = AF_INET;
	local.sin_port = htons(port);
	local.sin_addr.s_addr = INADDR_ANY;
	bind(fd, (struct sockaddr*)&local, sizeof(struct sockaddr_in));

	listen(fd, 20);
	printf("listen port : %d\n", port);

	
	struct timeval tv_begin;
	gettimeofday(&tv_begin, NULL);

	while (1) {
		socklen_t len = sizeof(struct sockaddr_in);
		int cli_fd = accept(fd, (struct sockaddr*)&remote, &len);
		if (cli_fd % 1000 == 999) { // 每创建1000个连接，统计一次耗时

			struct timeval tv_cur;
			memcpy(&tv_cur, &tv_begin, sizeof(struct timeval));
			
			gettimeofday(&tv_begin, NULL);
			int time_used = TIME_SUB_MS(tv_begin, tv_cur);
			
			printf("client fd : %d, time_used: %d\n", cli_fd, time_used);
		}
		printf("new client comming\n");

		coroutine *read_co;
		coroutine_create(&read_co, server_reader, &cli_fd); // 创建新的协程并加入调度器，执行与客户端的通信

	}
	
}



int main(int argc, char *argv[]) {
	coroutine *co = NULL;

	int i = 0;
	unsigned short base_port = 9096;
	for (i = 0;i < 100;i ++) {   // 创建100个端口上的监听套接字，每个监听套接字一个协程 ，开始套娃了
		unsigned short *port = calloc(1, sizeof(unsigned short));
		*port = base_port + i;
		coroutine_create(&co, server, port); ////////no run
	}

	schedule_run(); //run

	return 0;
}



