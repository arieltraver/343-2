//used tutorial here: https://www.linode.com/docs/guides/developing-udp-and-tcp-clients-and-servers-in-go/

package main

import (
	"github.com/arieltraver/343-2/helper"
	"bufio"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"regexp"
	"strconv"
	"strings"
)

// read the byte array sent from the leader and finds the frequency of each word.
func wordcount(b []byte) map[string]int {
	fmt.Println("mapping words")
	nonLetter, err := regexp.Compile("[^a-zA-Z0-9]")
	helper.CheckFatalErr(err)
	counts := make(map[string]int)
	str := string(b[:])                                           //break reading pattern into words
	word := strings.ToLower(nonLetter.ReplaceAllString(str, " ")) //get rid of extra characters
	words := strings.Fields(word)
	for _, wd := range words {
		counts[wd] += 1 // increment word count in the dictionary
	}

	return counts
}

// turns a hash map into a big string which can be sent across the net
func mapToString(counts map[string]int) *strings.Builder {
	var b strings.Builder // minimize memory copying
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
	helper.CheckFatalErrConn(c, err)
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
	helper.CheckFatalErrConn(c, err)
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
	_, err := io.WriteString(c, "ready\n") //send text to your connection
	helper.CheckFatalErrConn(c, err)
}

/*
waits for the leader to send "map words"
may want to try and make use of a timeout function
*/
func waitForJobName(c net.Conn) int {
	fmt.Println("ready sent, waiting for job")
	reader := bufio.NewReader(c)
	txt, err := reader.ReadString('\n')
	helper.CheckFatalErrConn(c, err)
	fmt.Println("text is:", txt)
	if strings.Trim(txt, " \n") == "DONE" {
		fmt.Println("all done!")
		return 0
	}
	if strings.Trim(txt, " \n") == "count words" { //c had random stuff for whatever reason
		fmt.Println("received order to count words!")
		chunksz, err := reader.ReadString('\n')
		helper.CheckFatalErrConn(c, err)
		chunkSize, err2 := strconv.Atoi(strings.Trim(chunksz, " \n"))
		helper.CheckFatalErrConn(c, err2)
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
	helper.CheckFatalErrConn(c, err)
	bytes := make([]byte, chunkSize)         //array of bytes
	bytesRead, err2 := io.ReadFull(c, bytes) //read server's message into bytes array
	helper.CheckFatalErrConn(c, err2)
	fmt.Println("bytes read:", bytesRead)
	return bytes
}
