package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"os"
	"strings"
)

var count = 0

func handleConnection(conn net.Conn) {
	for {
		netData, err := bufio.NewReader(conn).ReadString('\n')
		if err != nil {
			fmt.Println(err)
			return
		}

		temp := strings.TrimSpace(strings.ToUpper(string(netData)))
		fmt.Printf(temp)
		if temp == "STOP" {
			break
		}
		conn.Write([]byte(temp)) // echoing user input in uppercase
	}
	conn.Close()
}

func main() {
	arguments := os.Args
	if len(arguments) == 1 {
		fmt.Println("Please provide port number.")
		return
	}

	PORT := ":" + arguments[1]
	listener, err := net.Listen("tcp", PORT)
	if err != nil {
		log.Fatal(err)
	}
	defer listener.Close()

	for { // Endless loop because the server is constantly running
		//only stops if handleConnection() reads STOP
		c, err := listener.Accept()
		if err != nil {
			log.Fatal(err)

		}
		go handleConnection(c) // Each client served by a different goroutine
		count++
	}
}
