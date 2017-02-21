/* Header file to protocol */

#define PAXOS_STATUS_STARTUP	1
#define PAXOS_STATUS_PROMISED	2
#define PAXOS_STATUS_ACCEPTED	3

typedef struct MsgHeader
{
	char type;
	int from;
	int to;
} MsgHeader;

typedef struct MsgJoin
{
	MsgHeader header;
	int port;
} MsgJoin;

typedef struct MsgRequest
{
	MsgHeader header;
	char type;
	int to;
	int value;
} MsgRequest;

typedef struct MsgPrepare
{
	MsgHeader header;
	int i_id;
	int ballot;
} MsgPrepare;

typedef struct MsgPromised
{
	MsgHeader header;
	int i_id;
	int ballot;
	int v_ballot;	/* meaning accepted, if not invalid */
	int v_value;	/* accepted value */
	int reject;
} MsgPromised;

typedef struct MsgPropose
{
	MsgHeader header;
	int i_id;
	int ballot;
	int value;
} MsgPropose;

typedef struct MsgAccepted
{
	MsgHeader header;
	int i_id;
	int ballot;
	int value;
	int reject; /* 1 if reject */
} MsgAccepted;

typedef struct MsgLearn
{
	MsgHeader header;
	int i_id;
	int ballot;
	int value;
} MsgLearn;
