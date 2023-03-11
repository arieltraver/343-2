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
	//"time"
	//"math/rand"
)

//read the byte array sent from the leader and finds the frequency of each word.
func wordcount(b []byte) map[string]int {
	fmt.Println("mapping words")
	var nonLetter = regexp.MustCompile(`[^a-zA-Z0-9]+`)
	counts := make(map[string] int) //for the result
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
			counts[wd2] += 1 //increment word count in the dictionary
		}
	}
	return counts
}

//turns a hash map into a big string which can be sent across the net
func mapToString(counts map[string]int) *strings.Builder{
	var b strings.Builder //minimize memory copying
	for key, count := range(counts) {
		b.WriteString(key)
		b.WriteRune(':')
		b.WriteString(strconv.Itoa(count))
		b.WriteRune(' ')
	}
	b.WriteRune('\n')
	return &b
}

//takes a string builder and sends the string across c
func sendString(c net.Conn, str *strings.Builder){
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
	defer c.Close() //make sure it closes
	for {
		fmt.Println("sending ready") 
		sendReadies(c)
		waitForJobName(c) //needs an error return condition tree
		b := okAwaitBytes(c)
		if b == nil {
			return //this happens when we are all done.
		}
		m := wordcount(b)
		s := mapToString(m)
		sendString(c, s)
	}

}

/*sends "ready" periodically to the leader
randomly chooses how often to repeat the for loop*/
func sendReadies(c net.Conn) {
	_, err2 := io.WriteString(c, "ready\n") //send text to your connection
	if err2 != nil {
		log.Fatal(err2)
	}
}

/*waits for the leader to send "map words"
may want to try and make use of a timeout function */
func waitForJobName(c net.Conn) {
	txt, err := bufio.NewReader(c).ReadString('\n')
	if err != nil {
		c.Close()
		log.Fatal(err)
	}
	if strings.Trim(txt, " \n") == "count words" { //c had random stuff for whatever reason
		fmt.Println("received order to count words!")
		return
	}
}

//send the "ok map words"
//maybe want to include a timeout option
func okAwaitBytes(c net.Conn) []byte {
	chunkLength := 100 //temp, replace with parameter
	_, err := io.WriteString(c, "ok count words\n")
	if err != nil {
		c.Close()
		log.Fatal(err)
	}
	bytes := make([]byte, chunkLength) //array of bytes
	_, err2 := bufio.NewReader(c).Read(bytes) //read server's message into bytes array
	if err2 != nil {
		c.Close()
		log.Fatal(err)
	}
	if strings.ToUpper(string(bytes[0:4])) == "DONE" {
		fmt.Println("job is done!")
		return nil
	}
	return bytes
}


///linking code
///run "send readies" (loops)
///run 