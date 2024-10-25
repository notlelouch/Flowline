package main

import (
	"bufio"
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
	// DIREST CONNECTION READ/WRITE METHOD
	// length := make([]byte, 4)
	// _, err = conn.Read(length)
	// if err != nil {
	// 	return
	// }
	//
	// request := make([]byte, binary.BigEndian.Uint32(length))
	// _, err = conn.Read(request)
	// if err != nil {
	// 	return
	// }
	//
	// correlation_id := binary.BigEndian.Uint32(request[4:8])
	//
	// response := make([]byte, 8)
	// binary.BigEndian.PutUint32(response[4:], correlation_id)
	//
	// conn.Write(response)

	// USING BUFFERED I/O
	reader := bufio.NewReader(conn)
	peekedData, err := reader.Peek(4)
	if err != nil {
		fmt.Println("Error peeking data:", err)
		return
	}
	// fmt.Printf("Peeked data: %v", peekedData)

	request_length := binary.BigEndian.Uint32(peekedData)

	request := make([]byte, request_length)
	_, err = reader.Read(request)
	if err != nil {
		fmt.Println("Error reading request:", err)
		return
	}

	_, err = reader.Discard(4)
	if err != nil {
		return
	}

	request_api_version := int16(binary.BigEndian.Uint16(request[6:8]))
	correlation_id := binary.BigEndian.Uint32(request[8:12])

	fmt.Println(request)
	fmt.Println(correlation_id)
	fmt.Println(request_api_version)

	response := make([]byte, 10)
	binary.BigEndian.PutUint32(response[4:8], correlation_id)

	if 0 > request_api_version || request_api_version > 4 {
		binary.BigEndian.PutUint16(response[8:10], 35)
	} else {
		binary.BigEndian.PutUint16(response[8:10], 0)
	}

	fmt.Println(request)

	conn.Write(response)

	defer conn.Close()
}
