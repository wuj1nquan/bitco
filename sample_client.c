/*
 *  Author : WangBoJing , email : 1989wangbojing@gmail.com
 *
 */



#include "coroutine.h"

#include <arpa/inet.h>



#define SERVER_IPADDR		"127.0.0.1" // 本地回环地址
#define SERVER_PORT			9096

int init_client(void) { // 创建客户端套接字并连接服务器
 
	int clientfd = socket(AF_INET, SOCK_STREAM, 0);
	if (clientfd <= 0) {
		printf("socket failed\n");
		return -1;
	}

	struct sockaddr_in serveraddr = {0};
	serveraddr.sin_family = AF_INET;
	serveraddr.sin_port = htons(SERVER_PORT);
	serveraddr.sin_addr.s_addr = inet_addr(SERVER_IPADDR);

	int result = connect(clientfd, (struct sockaddr *)&serveraddr, sizeof(serveraddr));
	if (result != 0) {
		printf("connect failed\n");
		return -2;
	}

	return clientfd;
	
}

void client(void *arg) { // 循环向服务器发送消息

	int clientfd = init_client();
	char *buffer = "client\r\n";

	while (1) {

		int length = send(clientfd, buffer, strlen(buffer), 0);
		printf("echo length : %d\n", length);

		sleep(1);
	}

}



int main(int argc, char *argv[]) {
	coroutine *co = NULL;

	coroutine_create(&co, client, NULL); // 创建一个协程，执行client函数，并将其添加到其所在线程调度器的就绪队列
	
	schedule_run(); //运行调度器，调度其中的线程

	return 0;
}







