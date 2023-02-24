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
	}
	conn := args[1]
	c, err := net.Dial("tcp", conn) //connect to host:port
	if err != nil {
		log.Fatal(err)
	}
	fmt.Print("Welcome to the TCP client.\nType your message and hit enter.\nType STOP to stop.\n")

	for {
		fmt.Print(">>> ")
		scanner := bufio.NewScanner(os.Stdin) //read standard input
		if scanner.Scan() {
			txt := scanner.Text()
			fmt.Fprintf(c, txt+"\n")
			if strings.ToUpper(strings.TrimSpace(txt)) == "STOP" {
				fmt.Println("TCP client now exiting. Goodbye!")
				c.Close()
				return
			} 
		} //take in text until the newline
		if err := scanner.Err(); err != nil {
			log.Println(err)
			c.Close()
			return
		}
		serverScanner := bufio.NewScanner(c) //read what the server sends you
		if serverScanner.Scan() {
			msg := serverScanner.Text()
			fmt.Print("->: " + msg)  	
		}
		if err2 := serverScanner.Err(); err2 != nil {
			log.Println(err2)
			c.Close()
			return
		}
	}

}
