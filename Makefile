OBJS = paxos.c

paxos: ${OBJS}
	gcc -Wall -g -o paxos ${OBJS}
	gcc -Wall -g -o client client.c

clean:
	rm -f paxos client

