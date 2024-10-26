package main

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"net"
	"os"
)

func main() {
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
	defer conn.Close()

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

	// Response format:
	// Length (4 bytes)
	// Correlation ID (4 bytes)
	// Error Code (2 bytes)
	// Number of API Keys (1 byte)
	// API Key (2 bytes)
	// Min Version (2 bytes)
	// Max Version (2 bytes)
	// Tagged Fields (1 byte)
	// Throttle Time (4 bytes)
	// Tagged Fields (1 byte)
	responseLength := 19 // Total size excluding length prefix
	response := make([]byte, 4+responseLength)

	// Parsing message length
	binary.BigEndian.PutUint32(response[0:4], uint32(responseLength))

	// Parsing correlation_id
	correlation_id := binary.BigEndian.Uint32(request[8:12])
	binary.BigEndian.PutUint32(response[4:8], correlation_id)

	// Parsing request_api_version to check for error code
	request_api_version := int16(binary.BigEndian.Uint16(request[6:8]))
	if request_api_version < 0 || request_api_version > 4 {
		binary.BigEndian.PutUint16(response[8:10], 35)
	} else {
		binary.BigEndian.PutUint16(response[8:10], 0)
	}

	// Set number of API keys to 2
	// ##################################################### SOMETHING's MISSING!!! ##################################################
	// Why is this value only supposed to be 2?
	response[10] = 2

	// Set API key 18 (ApiVersions)
	binary.BigEndian.PutUint16(response[11:13], 18) // API Key
	binary.BigEndian.PutUint16(response[13:15], 3)  // Min Version
	binary.BigEndian.PutUint16(response[15:17], 4)  // Max Version

	// Set tagged fields
	response[17] = 0 // First tagged fields

	// Set throttle time
	binary.BigEndian.PutUint32(response[18:22], 0)

	// Set final tagged fields
	response[22] = 0

	fmt.Println(responseLength)
	fmt.Println(request)
	fmt.Println(correlation_id)
	fmt.Println(request_api_version)
	fmt.Println(response)
	fmt.Printf("%x\n", response)

	_, err = conn.Write(response)
	if err != nil {
		fmt.Println("Error writing response:", err)
		return
	}
	conn.Close()
}
