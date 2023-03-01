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

	/* for { // Endless loop because the server is constantly running
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
	} */
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
	fileSize := fileInfo.Size()                                                 // get file size
	chunkSize := int(math.Ceil(float64(float64(fileSize) / float64(numHosts)))) //size of chunk per each host
	fmt.Println("chunksize is", chunkSize)
	fmt.Println("filesize is", fileSize)

	buffers := make([][][]byte, numHosts)
	read := 0
	for i := 0; i < numHosts; i++ {
		partSize := int(math.Min(float64(chunkSize), float64(fileSize-int64(i*chunkSize))))
		buff := make([]byte, partSize)
		fmt.Println("length of buffer:", len(buff))

		fmt.Println("chunk num:", i+1)
		bytesRead, err := file.Read(buff) //read the length of buffer from file
		if err != nil {
			if err == io.EOF {
				fmt.Println("reached end of file, chunks read:", i)
				break
			} else {
				log.Fatal(err)
			}
		}
		read += bytesRead
		fmt.Println("bytes read:", bytesRead)
		fmt.Println("total bytes read:", read)
		buffers[i] = [][]byte{{0}, buff}
	}
	return &buffers
}
