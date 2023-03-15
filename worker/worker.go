//used tutorial here: https://www.linode.com/docs/guides/developing-udp-and-tcp-clients-and-servers-in-go/

package main

import (
	"../helper"
	"bufio"
	"bytes"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"regexp"
	"strconv"
	"strings"
	//"time"
	//"math/rand"
)

// read the byte array sent from the leader and finds the frequency of each word.
func wordcount(b []byte) map[string]int {
	fmt.Println("mapping words")
	var nonLetter = regexp.MustCompile(`[^a-zA-Z0-9]+`)
	counts := make(map[string]int)          //for the result
	byteReader := bytes.NewReader(b)        //reader class for byte array
	scanner := bufio.NewScanner(byteReader) //buffered i/o: creates a pipe for reading
	scanner.Split(bufio.ScanWords)          //break reading pattern into words
	for scanner.Scan() {                    //reads until EOF OR until the limit
		word := scanner.Text()
		word = strings.ToLower(word)                 //lowercase-ify
		word = nonLetter.ReplaceAllString(word, " ") //get rid of extra characters
		words := strings.Split(word, " ")            //split words by char
		for _, wd := range words {
			wd2 := nonLetter.ReplaceAllString(wd, "") //get rid of spaces
			counts[wd2] += 1                          //increment word count in the dictionary
		}
	}
	return counts
}

// turns a hash map into a big string which can be sent across the net
func mapToString(counts map[string]int) *strings.Builder {
	var b strings.Builder //minimize memory copying
	for key, count := range counts {
		b.WriteString(key)
		b.WriteRune(':')
		b.WriteString(strconv.Itoa(count))
		b.WriteRune(' ')
	}
	return &b
}

// takes a string builder and sends the string across c
func sendString(c net.Conn, builder *strings.Builder) {
	s := builder.String() //get string from the builder object
	fmt.Println("sending the string")
	bytesWritten, err := io.WriteString(c, s+"\n") //send string. assuming chunk size selected to be sendable
	helper.CheckFatalErr(c, err)
	fmt.Println("bytes written:", bytesWritten)
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
	defer c.Close() //make sure it closes
	for {
		fmt.Println("sending ready")
		sendReadies(c)
		chunkSize := waitForJobName(c) //needs an error return condition tree
		if chunkSize == 0 {
			return
		}
		b := okAwaitBytes(c, chunkSize)
		if b == nil || len(b) == 0 {
			return //this happens when we are all done.
		}
		m := wordcount(b)
		s := mapToString(m)
		sendString(c, s)
	}
}

/*
sends "ready" periodically to the leader
randomly chooses how often to repeat the for loop
*/
func sendReadies(c net.Conn) {
	_, err2 := io.WriteString(c, "ready\n") //send text to your connection
	if err2 != nil {
		c.Close()
		log.Fatal(err2)
	}
}

/*
waits for the leader to send "map words"
may want to try and make use of a timeout function
*/
func waitForJobName(c net.Conn) int {
	fmt.Println("ready sent, waiting for job")
	reader := bufio.NewReader(c)
	txt, err := reader.ReadString('\n')
	if err != nil {
		c.Close()
		log.Fatal("error reading from connection\n", err)
	}
	fmt.Println("text is:", txt)
	if strings.Trim(txt, " \n") == "DONE" {
		fmt.Println("all done!")
		return 0
	}
	if strings.Trim(txt, " \n") == "count words" { //c had random stuff for whatever reason
		fmt.Println("received order to count words!")
		chunksz, err := reader.ReadString('\n')
		helper.CheckFatalErr(c, err)
		chunkSize, err2 := strconv.Atoi(strings.Trim(chunksz, " \n"))
		helper.CheckFatalErr(c, err2)
		return chunkSize
	} else {
		c.Close()
		log.Fatal("unexpected message")
	}
	return 0
}

// send the "ok map words"
// maybe want to include a timeout option
func okAwaitBytes(c net.Conn, chunkSize int) []byte {
	_, err := io.WriteString(c, "ok count words\n")
	helper.CheckFatalErr(c, err)
	bytes := make([]byte, chunkSize)         //array of bytes
	bytesRead, err2 := io.ReadFull(c, bytes) //read server's message into bytes array
	helper.CheckFatalErr(c, err2)
	fmt.Println("bytes read:", bytesRead)
	return bytes
}
