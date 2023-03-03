//used tutorial here: https://www.linode.com/docs/guides/developing-udp-and-tcp-clients-and-servers-in-go/

package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"os"
	"io"
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
		//read your own text
		reader := bufio.NewReader(os.Stdin) //read input
		fmt.Print(">>> ")
		txt, err := reader.ReadString('\n') //take in text until the newline
		if err != nil {
			log.Fatal(err) //clientside logs are fatal, server not
			return
		}
		if strings.TrimSpace(string(txt)) == "STOP" { //if the user enters stop...
			fmt.Println("TCP client now exiting. Goodbye!")
			return
		}
		_, err2 := io.WriteString(c, txt)                     //send text to your connection
		if err2 != nil {
			log.Fatal(err2)
		}

		msg, err := bufio.NewReader(c).ReadString('\n') //read what the server sends you
		if err != nil {
			log.Fatal(err)
		}
		fmt.Print("->: " + msg)                       //print out the server's message
	}
}

func waitForJobName(c net.Conn) {
	for {
		txt, err := bufio.NewReader(c).ReadString('\n')
		if err != nil {
			c.Close()
			log.Fatal(err)
		}
		fmt.Println(txt)
		if txt == "count words" {
			fmt.Println("received order to count words!")
			return
		}
	}
}