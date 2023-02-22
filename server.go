package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"os"
	//"strconv"
	"strings"
)

var count = 0

func handleConnection(c net.Conn) {
	for {
		netData, err := bufio.NewReader(c).ReadString('\n')
		if err != nil {
			log.Fatal(err)
		}

		temp := strings.TrimSpace(string(netData))
		if temp == "STOP" {
			break
		}
		fmt.Println(strings.ToUpper(temp)) // echoing back user input in uppercase
		//counter := strconv.Itoa(count) + "\n"
		//c.Write([]byte(string(counter)))
	}
	c.Close()
}

func main() {
	arguments := os.Args
	if len(arguments) == 1 {
		fmt.Println("Please provide port number.")
		return
	}

	PORT := ":" + arguments[1]
	l, err := net.Listen("tcp4", PORT)
	if err != nil {
		log.Fatal(err)
	}
	defer l.Close()

	for { // Endless loop because the server is constantly running
		//only stops if handleConnection() reads STOP
		c, err := l.Accept()
		if err != nil {
			log.Fatal(err)
		}
		go handleConnection(c) // Each client served by a different goroutine
		count++
	}
}
