//used tutorial here: https://www.linode.com/docs/guides/developing-udp-and-tcp-clients-and-servers-in-go/

package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"os"
	"strings"
)

func main() {
	args := os.Args
	if len(args) <= 1 {
		log.Fatal("please provide host:port to connect to")
		return
	}
	conn := args[1]
	c, err := net.Dial("tcp", conn) //connect to host:port
	if err != nil {
		log.Fatal(err)
		return
	}
	fmt.Print("Welcome to the TCP client.\nType your message and hit enter.\nType STOP to stop.\n")
	for {
		reader := bufio.NewReader(os.Stdin) //read input
		fmt.Print(">>> ")
		txt, err := reader.ReadString('\n') //take in text until the newline
		if err != nil {
			log.Println(err)
			return
		}
		fmt.Fprintf(c, txt+"\n")                      //print connection and your text
		msg, _ := bufio.NewReader(c).ReadString('\n') //read what the server sends you
		fmt.Print("->: " + msg)                       //print out the server's message
		if strings.TrimSpace(string(txt)) == "STOP" { //if the user enters stop...
			fmt.Println("TCP client now exiting. Goodbye!")
			//c.Close() //close connection
			return
		}
	}

}
