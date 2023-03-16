//used tutorial here: https://www.linode.com/docs/guides/developing-udp-and-tcp-clients-and-servers-in-go/

package main

import (
	"bufio"
	//"bytes"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"regexp"
	"strconv"
	"strings"

	"github.com/arieltraver/343-2/helper"
)

// Reads the byte array sent from the leader and finds the frequency of each word.
// Stores word frequency counts in a hashmap that it returns.
func wordcount(b []byte) map[string]int {
	counts := make(map[string]int)
	nonLetter, err := regexp.Compile("[^a-zA-Z0-9]")
	helper.CheckFatalErr(err)

	// bytes to string
	str := string(b[:])
	// standardizing to lower case and replacing extra characters
	word := strings.ToLower(nonLetter.ReplaceAllString(str, " "))
	words := strings.Fields(word)

	for _, wd := range words {
		counts[wd] += 1 // increment word count in the dictionary
	}

	return counts
}

// Given a hashmap, writes its contents into a strings.Builder object.
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

// Takes a string Builder and sends the string across the connection.
func sendString(c net.Conn, builder *strings.Builder) {
	s := builder.String() // get string from the builder object
	fmt.Println("sending the string")
	bytesWritten, err := io.WriteString(c, s+"\n") // send string. assuming chunk size selected to be sendable
	helper.CheckFatalErrConn(c, err)
	fmt.Println("bytes written:", bytesWritten)
}

// Sends the "ready" keyword to the leader.
func sendReadies(c net.Conn) {
	_, err := io.WriteString(c, "ready\n") //send text to your connection
	helper.CheckFatalErrConn(c, err)
}

// Waits for the leader to send "count words" confirmation. Returns the
func waitForJobName(c net.Conn) int {
	fmt.Println("ready sent, waiting for job")
	reader := bufio.NewReader(c)
	txt, err := reader.ReadString('\n')
	helper.CheckFatalErrConn(c, err)
	if strings.Trim(txt, "\n") == "DONE" {
		fmt.Println("all done!")
		return 0
	}
	if strings.Trim(txt, "\n") == "count words" {
		fmt.Println("received order to count words!")
		chunksz, err := reader.ReadString('\n')
		helper.CheckFatalErrConn(c, err)
		chunkSize, err2 := strconv.Atoi(strings.Trim(chunksz, " \n"))
		helper.CheckFatalErrConn(c, err2)
		return chunkSize
	} else {
		c.Close()
		fmt.Println("huh?" + strings.Trim(txt, "\n"))
		log.Fatal("unexpected message")
	}
	return 0
}

// Sends "ok map words" confirmation signal to leader and waits to receive a
// file chunk from the leader. Reads the file chunk into a byte array of length
// chunkSize. Returns the byte array.
func okAwaitBytes(c net.Conn, chunkSize int) []byte {
	_, err := io.WriteString(c, "ok count words\n")
	helper.CheckFatalErrConn(c, err)
	bytes := make([]byte, chunkSize)         //array of bytes
	bytesRead, err2 := io.ReadFull(c, bytes) //read server's message into bytes array
	helper.CheckFatalErrConn(c, err2)
	fmt.Println("bytes read:", bytesRead)
	return bytes
}

func main() {
	args := os.Args
	if len(args) <= 1 {
		log.Fatal("please provide host:port to connect to")
		return
	}
	conn := args[1]
	c, err := net.Dial("tcp", conn) // connect to host:port
	helper.CheckFatalErrConn(c, err)
	defer c.Close() // make sure it closes
	for {
		sendReadies(c) // letting leader know it's ready
		chunkSize := waitForJobName(c)
		// file has been processed or leader did not confirm job
		if chunkSize == 0 {
			return
		}
		b := okAwaitBytes(c, chunkSize)
		if b == nil || len(b) == 0 {
			return // all done
		}
		m := wordcount(b)
		s := mapToString(m)
		sendString(c, s)
	}
}
