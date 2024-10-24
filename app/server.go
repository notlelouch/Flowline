package main

import (
	"encoding/binary"
	"fmt"
	"net"
	"os"
)

func main() {
	fmt.Println("Logs from your program will appear here!")

	l, err := net.Listen("tcp", "0.0.0.0:9092")
	if err != nil {
		fmt.Println("Failed to bind to port 9092")
		os.Exit(1)
	}
	conn, err := l.Accept()
	if err != nil {
		fmt.Println("Error accepting connection: ", err.Error())
		os.Exit(1)
	}
	// Reading request length
	length := make([]byte, 4)
	leng_conn_read, err := conn.Read(length)
	if err != nil {
		return
	}
	// Reading the rest of the request
	request := make([]byte, binary.BigEndian.Uint32(length))
	_, err = conn.Read(request)
	if err != nil {
		return
	}

	correlation_id := binary.BigEndian.Uint32(request[4:8])

	// fmt.Println(length)
	fmt.Println(leng_conn_read)
	// fmt.Println(request)
	// fmt.Println(correlation_id)

	response := make([]byte, 8)
	binary.BigEndian.PutUint32(response[4:], correlation_id)
	conn.Write(response)

	// conn.Peek()

	// fmt.Println(response)

	defer conn.Close()
}
