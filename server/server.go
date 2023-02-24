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
			log.Println(err) //prints to standard error
			return
		}

		temp := strings.TrimSpace(string(netData)) //remove whitespaces
		if temp == "STOP" {
			break
		}
		response := fmt.Sprintf(strings.ToUpper(temp))
		conn.Write([]byte(response)) // echoing user input in uppercase
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
	listener, err := net.Listen("tcp4", PORT)
	if err != nil {
		log.Fatal(err)
	}
	defer listener.Close()

	fmt.Println("listening on port", arguments[1])

	for { // Endless loop because the server is constantly running
		//only stops if handleConnection() reads STOP
		conn, err := listener.Accept()
		if err != nil {
			log.Println("failed connection")
		} else { //if one connection fails you can have more
			fmt.Println("new host joining:", conn.RemoteAddr())
			go handleConnection(conn) // Each client served by a different goroutine
			count++
		}
	}
}
