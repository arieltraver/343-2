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

//sendReadies()
//send "ready" every n tics to the server
//every time ready is sent, check if a job or 'done' is received
//if 'done' received, terminate
//continue until job received
//--> handshake()

//handshake:
//send 'ok word map', block until you receive a response.
//--> awaitBytes()

//awaitBytes:
//block until you receive bytes
//save them in local memory as a big string (or bytes array?)
//--> wordcount()

//wordcount()
//use bufio.ReadStrings (or bytes? is there a byte reader?) to read the big string
//use bufio split scanner to scan by word
//turn word lowercase
//save into hashmap
//--> mapToString()

//mapToString()
//turn the hash map into a string
//send the string to leader via net.conn (bufio.writeString?... etc)
//-->sendReadies()


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

func sendReadies(c net.Conn) {
	for {
		_, err2 := io.WriteString(c, "ready") //send text to your connection
		if err2 != nil {
			log.Fatal(err2)
		}
		msg, err := bufio.NewReader(c).ReadString('\n') //read what the server sends you
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println(msg)
		if strings.TrimSpace(string(msg))== "map words" { //if the user enters stop...
			fmt.Println("mapping words")
			return
		}
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

func okAwaitBytes(c net.Conn) []byte {
	chunkLength := 100 //temp, replace with parameter
	for { //do we need this or does it just block?
		_, err := io.WriteString(c, "ok map words")
		if err != nil {
			c.Close()
			log.Fatal(err)
		}
		bytes := make([]byte, chunkLength)
		_, err2 := bufio.NewReader(c).Read(bytes) //read what the server sends you
		if err2 != nil {
			c.Close()
			log.Fatal(err)
		}
		return bytes
	}
}