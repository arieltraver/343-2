package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
)

var count = 0

func handleConnection(c net.Conn) {
	fmt.Print(".")
	for {
		netData, err := bufio.NewReader(c).ReadString('\n') // if there's nothing to read, the code will stop at ln 34
		if err != nil {
			log.Fatal(err)
		}

		temp := strings.TrimSpace(string(netData))
		if temp == "STOP" {
			return
		}
		fmt.Println(temp)
		counter := strconv.Itoa(count) + "\n"

		c.Write([]byte(string(counter))) // write time to channel
	}
}

func main() {
	arguments := os.Args
	if len(arguments) == 1 {
		fmt.Println("Please provide port number.")
		return
	}

	PORT := ":" + arguments[1]
	l, err := net.Listen("tcp", PORT)
	if err != nil { // Port could be already in use, could be internal failure, could be overloaded with priorities
		fmt.Println(err)
		return
	}
	defer l.Close() // Delay the closure of the listener until after the function terminates, but
	// the port needs to be closed to ensure the computer has resources

	for { // endless loop because the server is constantly running - only stops if handleConnection() reads STOP
		c, err := l.Accept()
		if err != nil {
			fmt.Println(err)
			return
		}
		go handleConnection(c) // Each client served by a different goroutine
		count++
	}
}
