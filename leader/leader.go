package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	//"sync"
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

/**Reads a file into chunks and saves these chunks in memory
	this is gross bc you are going to need to request stack space
	parameters:
		-- directory: directory to read a file from
		-- numhosts: number of chunks
	returns:
		-- a 3d byte array. each host has one array of bytes, and one array which just contains status
		-- necessary because go does not allow multi-type arrays
		-- could replace with a struct but not now
**/
func splitAndSend(directory string, numHosts int) *[][][]byte {
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

	buffers := make([][][]byte, numHosts)
	for i := 0; i < numHosts; i++ {
		partSize := int(math.Min(float64(chunkSize), float64(fileSize-int64(i*chunkSize))))
		pair := make([][]byte, 2)
		pair[0] = []byte {0}
		buff := make([]byte, partSize)
		pair[1] = buff
		buffers[i] = pair //for each host, chunk and status
	}

	for i, pair := range(buffers) {
		buffer := pair[1] //get the buffer, not the status
		bytesRead, err := file.Read(buffer) //read the length of buffer from file
		if err != nil && err != io.EOF {
			log.Fatal(err)
		}
		fmt.Println("bytes read:", bytesRead)
		if err != io.EOF {
			fmt.Println("reached end of file, chunks read:", i)
			break;
		}
	}

	return &buffers

}
