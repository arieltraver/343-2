package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"io"
	"math"
)

var count = 0

func handleConnection(c net.Conn) {
	for {
		netData, err := bufio.NewReader(c).ReadString('\n')
		if err != nil {
			log.Println(err) //prints to standard error
			return
		}
		fmt.Print(string(netData))
		temp := strings.TrimSpace(strings.ToUpper(string(netData)))
		if temp == "STOP" {
			count--
			break
		}
		counter := strconv.Itoa(count) + "\n"
		fmt.Fprintf(c, counter) //send counter

	}
	c.Close()

}
func main() {
	arguments := os.Args
	if len(arguments) <= 1 {
		fmt.Println("Usage: 'leader host directory'")
		return
	}

	DIRECTORY := arguments[2]
	fmt.Println("directory:", DIRECTORY)
	PORT := ":" + arguments[1]
	listener, err := net.Listen("tcp4", PORT)
	if err != nil {
		log.Fatal(err)
		return
	}
	defer listener.Close()

	fmt.Println("listening on port", arguments[1])

	for { // Endless loop because the server is constantly running
		//only stops if handleConnection() reads STOP
		conn, err := listener.Accept()
		if err != nil {
			log.Println("failed connection")
			return
		} else { //if one connection fails you can have more
			fmt.Println("new host joining:", conn.RemoteAddr())
			go handleConnection(conn) // Each client served by a different goroutine
			count++
		}
	}
}

func splitAndSend(directory string, numHosts int) {
	fps, err := os.ReadDir(directory)
	if err != nil {
		log.Fatal(err)
	}
	file, err := os.Open(fps[0].Name())
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	fileInfo, err := file.Stat() // get file stats
	if err != nil {
		log.Fatal(err)
	}
	fileSize := fileInfo.Size() // get file size
	chunkSize := int(fileSize) / numHosts //size of chunk per each host
	//chunkSize += 1

	buffers := make([][]byte, numHosts)
	for i := 0; i < numHosts; i++ {
		partSize := int(math.Min(float64(chunkSize), float64(fileSize-int64(i*chunkSize))))
		buff := make([]byte, partSize)
		buffers[i] = buff
	}

	for i, buffer := range(buffers) {
		bytesRead, err := file.Read(buffer)
		if err != nil && err != io.EOF {
			log.Fatal(err)
		}
		fmt.Println("bytes read:", bytesRead)
		if err != io.EOF {
			fmt.Println("reached end of file, chunks read:", i)
			break;
		}
	}


}
