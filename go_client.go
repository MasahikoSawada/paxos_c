package main

import (
	"fmt"
	"flag"
	"net"
)

type MsgHeader struct {
	msgtype byte
	from, to int32
}

type MsgRequest struct {
	header MsgHeader
	ctype byte
	from int
	value int
}

var (
	cmd = flag.String("c", "d", "command")
	port = flag.String("p", "11110", "port number connect to")
	value = flag.Int("v", 999, "request value")
)



func main() {
	flag.Parse()

	var msg MsgRequest
	buf := make([]byte, 24)
	var i int = 0

	/* header */
	msg.header.msgtype = 'r'
	msg.header.from = -1
	msg.header.to = -1
	/* body */
	msg.ctype = ([]byte(*cmd))[0]
	msg.from = -1
	msg.value = *value
	fmt.Printf("port %s\n", *port)
	fmt.Printf("cmd %s\n", *cmd)
	fmt.Printf("value %d\n", *value)
	fmt.Println(msg)

	i = 0
	/* header is 12 byte */
	buf[i] = 'R' /* type */
	i += 4
	buf[i] = byte(100) /* from */
	i += 4
	buf[i] = byte(100) /* to */
	i += 4 /* add padding */
	
	buf[i] = ([]byte(*cmd))[0] /* command */
	i += 4
	buf[i] = 0 /* to */
	i += 4
	buf[i] = byte(*value) /* value */

	conn, err := net.Dial("tcp", "127.0.0.1" + ":" + *port)
	if err != nil {
		fmt.Printf("Dial error : %s\n", err)
		return
	}
	defer conn.Close()

	conn.Write(buf)
}
