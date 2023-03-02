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
	//"math"
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

	PORT := ":" + arguments[1]
	listener, err := net.Listen("tcp4", PORT)
	if err != nil {
		log.Fatal(err)
		return
	}
	defer listener.Close()
	fmt.Println("listening on port", arguments[1])

	DIRECTORY := arguments[2]
	fmt.Println("directory:", DIRECTORY)
	readAndSplit(DIRECTORY, 10)

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

/*
  - Reads a file into chunks and saves these chunks in memory.
    this is gross bc you are going to need to request stack space.
    Returns a 3d byte array. each host has one array of bytes, and one array which just contains status

*
*/
func readAndSplit(directory string, numHosts int) *[][][]byte {
	fps, err := os.ReadDir(directory)
	if err != nil {
		log.Fatal(err)
	}
	file, err := os.Open(directory + "/" + fps[0].Name())
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	fileInfo, err := file.Stat() // get file stats
	if err != nil {
		log.Fatal(err)
	}
<<<<<<< HEAD
	fileSize := fileInfo.Size() // get file size
	chunkSize := int(fileSize) / numHosts + 1 //size of chunk per each host
	fmt.Println("chunksize is", chunkSize)
=======

	fileSize := fileInfo.Size()                                        // get file size
	chunkSize := int(math.Ceil(float64(fileSize) / float64(numHosts))) //size of chunk per each host
	fmt.Println("chunksize is", chunkSize, "| filesize is", fileSize)
>>>>>>> 6b9acb749cd4c88d9cde1baac925f64c7f4f6047

	buffers := make([][][]byte, numHosts)
	read := 0
	for i := 0; i < numHosts; i++ {
<<<<<<< HEAD
		//partSize := int(math.Min(float64(chunkSize), float64(fileSize-int64(i*chunkSize)))) //limits size to remaining
		pair := make([][]byte, 2)
		pair[0] = []byte {0}
		buff := make([]byte, chunkSize)
		pair[1] = buff
		buffers[i] = pair //for each host, chunk and status
	}

	for i, pair := range(buffers) {
		buffer := pair[1] //get the buffer, not the status
		fmt.Println("length of buffer:", len(buffer))
		bytesRead, err := file.Read(buffer) //read the length of buffer from file
		if err != nil && err != io.EOF {
			log.Fatal(err)
		} else if err == io.EOF {
			fmt.Println("reached end of file, chunks read:", i)
			fmt.Println(buffer[len(buffer)-1]) //see if this is full
			break;
		}
		fmt.Println("bytes read:", bytesRead)
=======
		//partSize := int(math.Min(float64(chunkSize), float64(fileSize-int64(i*chunkSize))))
		buff := make([]byte, chunkSize)
		fmt.Println("length of buffer:", len(buff))
		fmt.Println("chunk num:", i+1)

		bytesRead, err := file.Read(buff) //read the length of buffer from file
		if err != nil {
			if err == io.EOF {
				fmt.Println("reached end of file, chunks read:", i+1)
				break
			} else {
				log.Fatal(err)
			}
		}
		read += bytesRead
		fmt.Println("bytes read:", bytesRead)
		buffers[i] = [][]byte{{0}, buff}
>>>>>>> 6b9acb749cd4c88d9cde1baac925f64c7f4f6047
	}
	fmt.Println("total bytes read:", read)
	return &buffers
}
