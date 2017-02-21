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

#define BUFSIZE 8192
#define InvalidSocket -1
#define InvalidBallot -1

#define getInstance(i_id) &(PaxosInst[(i_id)])
#define getBallotNumber(prev_b) ((prev_b) + 100 + ServId)

#define init_message(header, mtype, dist) \
do { \
	((MsgHeader) (header)).type = (mtype); \
	((MsgHeader) (header)).from = ServId; \
	((MsgHeader) (header)).to = (dist); \
} while(0)

#define DEBUG(fmt, ...) \
do { \
	if (Verbose) \
		printf("<%d>: "fmt, ServId, __VA_ARGS__);\
} while(0)

typedef struct PaxosNode
{
	int id;
	struct sockaddr_in addr;
	int port;
	int socket;
} PaxosNode;

typedef struct Instance
{
	int i_id;	/* instance id */
	int ballot;	/* ballot number */
	int value;
	int status;

	/* Stats */
	int n_votes;
	int n_sents;
	int max_b;	/* max ballot number already accepted */
	int max_b_value; /* max value corresponding max_b */
} Instance;

/* Server global variables */
int ServId;
int ServSock;
int ServPort;
struct sockaddr_in servAddr;
fd_set ReadFds;
char *ConfigFile = "cluster.conf";
int ProposeValue;
int Verbose = 1;

/*
 * Paxos nodes definitions.
 *
 * CurrentInstId is the instance number that we're getting consensus.
 * IOW, we ensure that the instances < CurrentInstId have gotten consensus.
 *
 * xxx : need to support flexible length array.
 */
int n_nodes = 0;
PaxosNode PaxosNodes[128] = {};
Instance PaxosInst[8192] = {};
int CurrentInstId = -1;

static void sigint_handler(int signal);

static int prepareSocket(struct sockaddr_in *addr, int port);
static int sendMessage(PaxosNode *node, char *msg, int bytes);
static int initMask(void);
static PaxosNode *addPaxosNode(int id, struct sockaddr_in addr, int socket,
							   int port);
static char *getMessage(PaxosNode *node);
static char *getMessageFromSocket(int socket);
static void processMessage(char *msg, PaxosNode *node);
static void MainLoop(void);
static PaxosNode *getPaxosNodesById(int id);
static void usage(void);

static void paxosPrepare(int i_id, int ballot);
static void paxosPropose(int i_id, int ballot, int value);
static void paxosLearn(int i_id, int ballot, int value);

/* For debugging */
static void dump_paxos_nodes(void);

int
main(int argc, char **argv)
{
	char opt;
	FILE *fp;
	char buf[BUFSIZE];
	int num = 0;

	while((opt = getopt(argc, argv, "p:i:f:vh")) != -1)
	{
		switch(opt)
		{
			case 'p':
				ServPort = atoi(optarg);
				break;
			case 'i':
				ServId = atoi(optarg);
				break;
			case 'f':
				ConfigFile = strdup(optarg);
				break;
			case 'v':
				Verbose = 0;
				break;
			case 'h':
				usage();
				exit(0);
			default:
				fprintf(stderr, "invalid option :%c\n", opt);
		}
	}

	if (ServPort < 0 || ServId < 0)
	{
		fprintf(stderr, "port number(-p) and ID(-i) must be specified\n");
		exit(1);
	}

	ServSock = prepareSocket(&servAddr, ServPort);
	FD_ZERO(&ReadFds);
	FD_SET(ServSock, &ReadFds);

	/* Bind socket to addr */
	if (bind(ServSock, (struct sockaddr *) &servAddr, sizeof(servAddr)) < 0)
	{
		fprintf(stderr, "error : bind %d\n", errno);
		exit(1);
	}

	/* Listen socket */
	if (listen(ServSock, 5) < 0)
	{
		fprintf(stderr, "error : listen %d\n", errno);
		exit(1);
	}

	/* Load config file */
	if ((fp = fopen(ConfigFile, "r")) == NULL)
	{
		fprintf(stderr, "read error : %d\n", errno);
		return -1;
	}
	while (fgets(buf, BUFSIZE, fp) != NULL)
	{
		MsgJoin mjoin;
		PaxosNode n;
		char  *id_str, *port_str;
		char *p;
		int ret;

		if (buf[0] == '\0' || buf[0] == '\n')
			continue;

		/* Parse a line */
		p = buf;
		id_str = p;		/* Get id */
		p++;
		*p = '\0';	/* Replace connma with \0 */
		p++;
		port_str = p;	/* Get port number */

		n.id = atoi(id_str);
		n.socket = InvalidSocket;
		n.port = atoi(port_str);
		n.addr.sin_family = PF_INET;
		n.addr.sin_addr.s_addr = inet_addr("127.0.0.1");
		n.addr.sin_port = htons(atoi(buf));

		/* check if myself */
		if (n.id == ServId)
		{
			/* Register myself */
			addPaxosNode(n.id, n.addr, InvalidSocket, n.port);
			continue;
		}

		/* Join paxos group, try to connecto other nodes */
		init_message(mjoin.header, 'j', num);
		mjoin.port = ServPort;
		ret = sendMessage(&n, (char *) &mjoin, sizeof(MsgJoin));

		/* Add node if 'join' message success */
		if (ret == 0)
			addPaxosNode(n.id, n.addr, n.socket, n.port);
	}

	/* Set signal handler */
	signal(SIGINT, sigint_handler);

	MainLoop();

	close(ServSock);
	return 0;
}

static PaxosNode *
addPaxosNode(int id, struct sockaddr_in addr, int socket, int port)
{
	PaxosNode *n;

	PaxosNodes[n_nodes].id = id;
	PaxosNodes[n_nodes].addr = addr;
	PaxosNodes[n_nodes].socket = socket;
	PaxosNodes[n_nodes].port = port;
	FD_SET(socket, &ReadFds);
	n = &(PaxosNodes[n_nodes]);
	n_nodes++;

	return n;
}

static void
MainLoop(void)
{
	fd_set fds;

	while(1)
	{
		int maxSocket;
		int ret;
		struct timeval time = {60, 0};

		memcpy(&fds, &ReadFds, sizeof(fd_set));
		maxSocket = initMask();
		ret = select(maxSocket + 1, &fds, NULL, NULL, &time);

		/* Get notification */
		if (ret != 0)
		{
			int i;

			/* Get connect to server socket */
			if (FD_ISSET(ServSock, &fds))
			{
				struct sockaddr_in client;
				socklen_t client_len = sizeof(client);
				int client_socket;
				PaxosNode *n = NULL;
				char *msg;

				if ((client_socket = accept(ServSock, (struct sockaddr *) &client, &client_len)) == -1)
				{
					fprintf(stderr, "error : accept %d\n", errno);
					return;
				}

				/* Process message, fist */
				msg = getMessageFromSocket(client_socket);

				/* Add paxos node if 'join' message, port number is filled later */
				if (((MsgHeader *)msg)->type == 'j')
					n = addPaxosNode(0, client, client_socket, -1);

				/*
				 * Client connectio is a temporal connection. To distinguish from
				 * paxos connection, process client message here without adding
				 * new paxos node.
				 */
				processMessage(msg, n);
			}

			for (i = 0; i < n_nodes; i++)
			{
				PaxosNode *n = &(PaxosNodes[i]);

				if (FD_ISSET(n->socket, &fds))
				{
					char *msg;

					if ((msg = getMessage(n)) == NULL)
					{
						/* Disconnected, remove paxos node */
						fprintf(stderr, "disconnected : id = %d, port = %d\n",
								n->id, n->port);
						FD_CLR(n->socket, &ReadFds);

						/* Shrink PaxosNodes array */
						memmove(&(PaxosNodes[i+1]),
								&(PaxosNodes[i]),
								sizeof(PaxosNode) * (n_nodes - i + 1));
						n_nodes--;
						continue;
					}

					processMessage(msg, n);
				}
			}
		}
		/* select(2) timed out */
	}
}

static int
prepareSocket(struct sockaddr_in *addr, int port)
{
	int sock;
	int on = 1;

	/* Create server socket */
	if ((sock = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP)) < 0)
	{
		fprintf(stderr, "error : socket %d\n", errno);
		exit(1);
	}

	setsockopt(sock,
			   SOL_SOCKET, SO_REUSEADDR, (const char *)&on, sizeof(on));

	memset((char *)addr, 0, sizeof(*addr));
	addr->sin_family = PF_INET;
	addr->sin_addr.s_addr = inet_addr("127.0.0.1");
	addr->sin_port = htons(port);

	return sock;
}

static int
sendMessage(PaxosNode *node, char *msg, int bytes)
{
	int socket;
	struct sockaddr_in addr;

	/* Exit if send to myself */
	if (node->id == ServId)
	{
		fprintf(stderr, "error : send myself\n");
		return -1;
	}

	/* Prepare socket for write if not exists */
	if (node->socket == InvalidSocket)
	{
		socket = prepareSocket(&addr, node->port);

		if (connect(socket, (struct sockaddr *) &addr, sizeof(addr)) < 0)
		{
			fprintf(stderr, "error : connect id = %d, port = %d : %d\n",
					node->id, node->port, errno);
			return -1;
		}

		/* Save write socket */
		node->socket = socket;
	}

	if (write(node->socket, msg, bytes) != bytes)
	{
		fprintf(stderr, "error : send msg %d\n", errno);
		return -1;
	}

	return 0;
}

static int
initMask(void)
{
	int i;
	int maxfd = ServSock;

	for (i = 0; i < n_nodes; i++)
	{
		if (maxfd < PaxosNodes[i].socket)
			maxfd = PaxosNodes[i].socket;
	}

	return maxfd;
}

static char *
getMessage(PaxosNode *node)
{
	char buf[BUFSIZE];
	char *msg;
	int bytes;

	/* Receive data */
	bytes = recv(node->socket, buf, sizeof(buf), 0);

	if (bytes == 0)
		return NULL;

	if (bytes < 0)
	{
		fprintf(stderr, "error : recv %d\n", errno);
		return NULL;
	}

	msg = malloc(sizeof(char) * bytes + 1);
	memcpy(msg, buf, bytes);
	msg[bytes] = '\0';

	return msg;
}

static char *
getMessageFromSocket(int socket)
{
	char buf[BUFSIZE];
	char *msg;
	int bytes;

	/* Receive data */
	bytes = recv(socket, buf, sizeof(buf), 0);

	if (bytes == 0)
		return NULL;

	if (bytes < 0)
	{
		fprintf(stderr, "error : recv %d\n", errno);
		return NULL;
	}

	msg = malloc(sizeof(char) * bytes + 1);
	memcpy(msg, buf, bytes);
	msg[bytes] = '\0';

	return msg;
}


static void
processMessage(char *msg, PaxosNode *node)
{
	char msgtype = ((char *)msg)[0];

	switch(msgtype)
	{
		case 'j': /* join */
			{
				MsgJoin *mjoin = (MsgJoin *) msg;

				DEBUG("recv \"%c\" (from id:port = %d:%d): port is \"%d\"\n",
					  msgtype, node->id, mjoin->port, mjoin->port);

				/*
				 * Register Paxos node. Since socket is already saved
				 * when accepted, save id and port which are set in message.
				 */
				node->id = mjoin->header.from;
				node->port = mjoin->port;
				break;
			}
		case 'i': /* Ping */
			{
				MsgHeader *ping = (MsgHeader *) msg;
				MsgHeader pong;

				DEBUG("recv \"%c\" (from id = %d): Ping FROM \"%d\" to \"%d\"\n",
					  msgtype, ping->from, ping->from, ping->to);

				init_message(pong, 'i', ping->from);
				sendMessage(node, (char *)&pong, sizeof(MsgHeader));
				break;
			}
		case 'p': /* Prepare */
			{
				MsgPrepare *prepare = (MsgPrepare *) msg;
				Instance *inst = getInstance(prepare->i_id);
				MsgPromised promised;

				DEBUG("recv \"%c\" (from id = %d): Prepare i_id = %d, ballot = %d\n",
					  msgtype, prepare->header.from, prepare->i_id, prepare->ballot);

				if (inst->ballot < prepare->ballot)
				{
					DEBUG("    -> prepared (inst:%d,prepare:%d)\n",
						  inst->ballot, prepare->ballot);
					/* Prepared if prepare ballot is higher than itself */
					init_message(promised.header, 'P', prepare->header.from);
					promised.i_id = prepare->i_id;
					promised.ballot = prepare->ballot;
					promised.v_ballot = InvalidBallot; /* Means, not accepted yet */
					promised.reject = 0;

					/* Already prepared, return with accepted value  */
					if (inst->status == PAXOS_STATUS_ACCEPTED)
					{
						promised.v_ballot = inst->ballot;
						promised.v_value = inst->value;
					}
				}
				else
				{
					DEBUG("    -> reject (inst:%d,prepare:%d)\n",
						  inst->ballot, prepare->ballot);
					/* Reject, return ballot number */
					init_message(promised.header, 'P', prepare->header.from);
					promised.i_id = prepare->i_id;
					promised.ballot = prepare->ballot; /* invalid ballot means rejected */
					promised.reject = 1;
				}

				sendMessage(node, (char *) &promised, sizeof(MsgPromised));
			}
			break;
		case 'P' : /* Promised */
			{
				MsgPromised *mpro = (MsgPromised *) msg;
				Instance *inst = getInstance(mpro->i_id);

				DEBUG("recv \"%c\" (from id = %d): Promised i_id = %d, ballot = %d, reject = %d\n",
					  msgtype, mpro->header.from,
					  mpro->i_id, mpro->ballot, mpro->reject);

				/* Prepare has been promised */
				if (mpro->reject == 0)
				{
					/*
					 * Since the prepare message has been promised by acceptor, we can
					 * do propose if not yet to do that. If this instance is already
					 * promised and remaining promised message has arrived, we can ignore
					 * it.
					 */
					if (inst->status != PAXOS_STATUS_PROMISED)
					{
						(inst->n_votes)++; /* increment vote */
						DEBUG("    -> accepted, votes:%d, sents:%d\n",
							  inst->n_votes, inst->n_sents);

						/*
						 * Remember the max ballot and correponding value if this instance
						 * has been already accepted. These max_b and max_b_value are used
						 * when proposing later.
						 */
						if (mpro->v_ballot != InvalidBallot &&
							inst->max_b < mpro->ballot)
						{
							inst->max_b = mpro->v_ballot;
							inst->max_b_value = mpro->v_value;
						}

						/* Propose if we got promised from the majority */
						if (inst->n_votes > (inst->n_sents / 2))
						{
							/*
							 * xxx : need to support time out.
							 * xxx : need to support to select highest ballot from all replies.
							 */
							/* Update status */
							inst->status = PAXOS_STATUS_PROMISED;

							if (mpro->v_ballot == InvalidBallot)
								/* this instance is not accepted yet, can propose own value */
								paxosPropose(mpro->i_id, mpro->ballot, ProposeValue);
							else
								/* Already accepted, propose with highest ballot and value */
								paxosPropose(mpro->i_id, inst->max_b, inst->max_b_value);
						}
					}
					else
						/* Already propose this instance, ignore it */
						DEBUG("        -> already promised ignore it, status %d\n", inst->status);
				}
				else
				{
					DEBUG("    -> rejected, again based on %d\n",
							mpro->ballot);
					/* prepare has been rejected, retry prepare again */
					paxosPrepare(mpro->i_id, getBallotNumber(mpro->ballot));
				}
				break;
			}
		case 'r': /* Propose */
			{
				MsgPropose *mprop = (MsgPropose *) msg;
				MsgAccepted macep;
				Instance *inst = getInstance(mprop->i_id);

				DEBUG("recv \"%c\" (from id = %d): Propose i_id = %d, ballot = %d, value = %d\n",
					  msgtype, mprop->header.from,
					  mprop->i_id, mprop->ballot,
					  mprop->value);

				init_message(macep.header, 'a', mprop->header.from);

				if (inst->ballot <= mprop->ballot)
				{
					/*
					 * If ballot of propose is higher than the ballot we already
					 * prepared, accept it.
					 */
					inst->ballot = mprop->ballot;
					inst->value = mprop->value;

					macep.i_id = mprop->i_id;
					macep.ballot = mprop->ballot;
					macep.value = mprop->value;
					macep.reject = 0;
				}
				else
				{
					/* Otherwise, reject it */
					macep.i_id = mprop->i_id;
					macep.ballot = InvalidBallot;
					macep.reject = 0;
				}

				sendMessage(node, (char *) &macep, sizeof(MsgAccepted));
				break;
			}
		case 'a': /* Accepted */
			{
				MsgAccepted *macep = (MsgAccepted *) msg;
				Instance *inst = getInstance(macep->i_id);

				DEBUG("recv \"%c\" (from id = %d): Accepted i_id = %d, ballot = %d, value = %d\n",
					  msgtype, macep->header.from,
					  macep->i_id, macep->ballot,
					  macep->value);

				if (inst->status != PAXOS_STATUS_ACCEPTED)
				{
					(inst->n_votes)++;
					DEBUG("    -> accepted votes:%d, sents:%d\n",
						  inst->n_votes, inst->n_sents);

					if (inst->n_votes > (inst->n_sents / 2))
					{
						inst->status = PAXOS_STATUS_ACCEPTED;

						/* Update current instance in me */
						inst->ballot = macep->ballot;
						inst->value = macep->value;

						/* Broadcast to leaner */
						paxosLearn(macep->i_id, macep->ballot, macep->value);
					}
				}
			break;
			}
		case 'l': /* Learn */
			{
				MsgLearn *mlearn = (MsgLearn *) msg;
				Instance *inst = getInstance(mlearn->i_id);

				DEBUG("recv \"%c\" (from id = %d): Learn i_id = %d, ballot = %d, value = %d\n",
					  msgtype, mlearn->header.from,
					  mlearn->i_id, mlearn->ballot,
					  mlearn->value);
				inst->status = PAXOS_STATUS_ACCEPTED;
				inst->ballot = mlearn->ballot;
				inst->value = mlearn->value;
				CurrentInstId++;
				break;
			}
		case 'R': /* Request from client */
			{
				MsgRequest *mreq = (MsgRequest *) msg;

				switch(mreq->type)
				{
					case 'd': /* Dump */
						dump_paxos_nodes();
						break;
					case 'i': /* Ping */
						{
							MsgHeader ping;
							MsgHeader *pong;
							PaxosNode *n;

							/* Get corresponding paxos node */
							n = getPaxosNodesById(mreq->to);

							/* make ping message */
							init_message(ping, 'i', mreq->to);
							DEBUG("send ping to id = %d --> ", mreq->to);

							sendMessage(n, (char *)&ping, sizeof(MsgHeader));

							/* get Pong message */
							pong = (MsgHeader *) getMessage(n);
							DEBUG("get pong, from %d\n", pong->from);
							break;
						}
					case 's': /* start paxos instance */
						{
							Instance *inst;

							CurrentInstId++;
							inst = getInstance(CurrentInstId);

							/* Check if given instance is already accepted */
							if (inst->status == PAXOS_STATUS_PROMISED)
							{
								DEBUG("instance %d is already accepted\n", inst->i_id);
								break;
							}

							/* Save value */
							ProposeValue = mreq->value;

							/* Do prepare */
							inst->status = PAXOS_STATUS_STARTUP;
							paxosPrepare(CurrentInstId, getBallotNumber(inst->ballot));
							break;
						}
					case 'r': /* Show result */
						{
							int i;

							for (i = 0; i <= CurrentInstId; i++)
							{
								Instance *inst = getInstance(i);

								printf("[%d] status = %d, ballot = %d, value = %d\n",
									   i, inst->status, inst->ballot, inst->value);
							}
						}
				}
			}
			break;
		default:
			fprintf(stderr, "error : invalid message type %c\n",
					((MsgHeader *) msg)->type);
	}

	return;
}

static void
sigint_handler(int signal)
{
	assert(signal == SIGINT);

	fprintf(stderr, "close server socket\n");
	close(ServSock);
	exit(0);
}

static PaxosNode *
getPaxosNodesById(int id)
{
	int i;
	for (i = 0; i < n_nodes; i++)
	{
		PaxosNode *n = &(PaxosNodes[i]);

		/* found */
		if (n->id == id)
			return n;
	}

	return NULL;
}

static void
dump_paxos_nodes(void)
{
	int i;

	fprintf(stderr, "------- DUMP --------\n");

	for (i = 0; i < n_nodes; i++)
	{
		PaxosNode *n = &(PaxosNodes[i]);

		fprintf(stderr, "[%d] id = %d, port = %d\n",
				i, n->id, n->port);
	}
}

static void usage(void)
{
	printf("Usage:\n");
	printf(" paxos [OPTION]\n");
	printf("\nOptions:\n");
	printf("  -i ID      Paxos ID\n");
	printf("  -p PORT    Listen port\n");
}

static void
paxosPrepare(int i_id, int ballot)
{
	MsgPrepare prepare;
	Instance *inst = getInstance(i_id);
	int i;

	DEBUG("        -> Do Prepare with %d\n", ballot);

	inst->n_votes = 1; /* vote by myself */
	inst->n_sents = 1; /* initialize */

	/* Broacast prepare message */
	for (i = 0; i < n_nodes; i++)
	{
		PaxosNode *node = &(PaxosNodes[i]);

		/* Skip myself */
		if (node->id == ServId)
			continue;

		init_message(prepare.header, 'p', node->id);
		prepare.i_id = i_id;
		prepare.ballot = ballot;

		sendMessage(node, (char *)&prepare, sizeof(MsgPrepare));
		(inst->n_sents)++;
	}
}

static void
paxosPropose(int i_id, int ballot, int value)
{
	MsgPropose propose;
	Instance *inst = getInstance(i_id);
	int i;

	DEBUG("        -> Do Propose with %d\n", ballot);

	inst->n_votes = 1; /* vote by myself */
	inst->n_sents = 1; /* initialize */

	/* Broacast propose message */
	for (i = 0; i < n_nodes; i++)
	{
		PaxosNode *node = &(PaxosNodes[i]);

		/* Skip myself */
		if (node->id == ServId)
			continue;

		init_message(propose.header, 'r', node->id);
		propose.i_id = i_id;
		propose.ballot = ballot;
		propose.value = value;

		sendMessage(node, (char *) &propose, sizeof(MsgPropose));
		(inst->n_sents)++;
	}
}

static void
paxosLearn(int i_id, int ballot, int value)
{
	MsgLearn mlearn;
	int i;

	/* Broacast propose message */
	for (i = 0; i < n_nodes; i++)
	{
		PaxosNode *node = &(PaxosNodes[i]);

		/* Skip myself */
		if (node->id == ServId)
			continue;

		init_message(mlearn.header, 'l', node->id);
		mlearn.i_id = i_id;
		mlearn.ballot = ballot;
		mlearn.value = value;

		sendMessage(node, (char *) &mlearn, sizeof(MsgLearn));
	}
}
