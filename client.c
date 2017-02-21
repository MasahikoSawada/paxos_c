#include <arpa/inet.h>
#include <assert.h>
#include <netinet/in.h>
#include <errno.h>
#include <signal.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/select.h>
#include <sys/time.h>
#include <unistd.h>

#include "proto.h"

int
main(int argc, char **argv)
{
	int sock;
	struct sockaddr_in addr;
	MsgRequest msg;
	int id;
	char opt;
	char req;
	int port;
	int value = 999;

	while ((opt = getopt(argc, argv, "i:p:v:dsr")) != -1)
	{
		switch(opt)
		{
			case 'i':
				id = atoi(optarg);
				break;
			case 'p':
				port = atoi(optarg);
				break;
			case 'd':
				req = 'd';
				break;
			case 's':
				req = 's';
				break;
			case 'r':
				req = 'r';
				break;
			case 'v':
				value = atoi(optarg);
				break;
			default:
				fprintf(stderr, "Invalid option : \"%c\\n", opt);
		}
	}

	/* make socket */
	if ((sock = socket(PF_INET, SOCK_STREAM, 0)) < 0)
	{
		/* error */
		fprintf(stderr, "error socket() %d ¥n", sock);
		return 0;
	}

	memset((char *) &addr, 0, sizeof(addr));
	addr.sin_family = PF_INET;
	addr.sin_addr.s_addr = inet_addr("127.0.0.1");
	addr.sin_port = htons(port);

	/* connect */
	if (connect(sock, (struct sockaddr * )&addr, sizeof(addr)) != 0)
	{
		/* error */
		fprintf(stderr, "error1 %d¥n", errno);
		return 1;
	}

	msg.header.type = 'R';
	msg.header.to = id;
	msg.header.from = -1;

	msg.type = req;
	switch(req)
	{
		case 'd': /* dump */
			/* nothing */
			break;
		case 's': /* start instance */
			msg.value = value;
			break;
		case 'r': /* show result */
			/* nothing */
			break;
	}
	
	/* send message */
	if (write(sock, &msg, sizeof(MsgRequest)) != sizeof(MsgRequest))
	{
		fprintf(stderr, "write error %d\n", errno);
		close(sock);
		return 1;
	}

	close(sock);
	return 1;
}
