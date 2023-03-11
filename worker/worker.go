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
	"bytes"
	"regexp"
	"strconv"
)

//read the byte array sent from the leader
func wordcount(b []byte) map[string]int {
	var nonLetter = regexp.MustCompile(`[^a-zA-Z0-9]+`)
	counts := make(map [string] int) //for the result
	byteReader := bytes.NewReader(b) //reader class for byte array
	scanner := bufio.NewScanner(byteReader) //buffered i/o: creates a pipe for reading
	scanner.Split(bufio.ScanWords) //break reading pattern into words
	for scanner.Scan() { //reads until EOF OR until the limit
		word := scanner.Text()
		word = strings.ToLower(word) //lowercase-ify
		word = nonLetter.ReplaceAllString(word, " ") //get rid of extra characters
		words := strings.Split(word, " ") //split words by char
		for _, wd := range(words) {
			wd2 := nonLetter.ReplaceAllString(wd, "") //get rid of spaces
			counts[wd2] = counts[wd2] + 1 //increment word count in the dictionary
		}
	}
	return counts
}

//turns a hash map into a big string which can be sent across the net
func mapToString(counts map[string]int) *strings.Builder{
	var b strings.Builder //minimize memory copying
	for key, count := range(counts) {
		b.WriteString(key)
		b.WriteRune(' ')
		b.WriteString(strconv.Itoa(count))
		b.WriteRune(',')
	}
	return &b
}

//takes a string builder and sends the string across c
func sendString(c net.Conn, str *strings.Builder) {
	s := str.String() //get string from the builder object
	_, err2 := io.WriteString(c, s) //send string. assuming chunk size selected to be sendable
	if err2 != nil {
		c.Close()
		log.Fatal(err2)
	}
}


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