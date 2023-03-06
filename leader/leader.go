package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
	//"math"
)

type Job struct {
	Id int
	Wg *sync.WaitGroup
}
type Leader struct {
	freqMap   map[string]int
	m         *sync.RWMutex
	jobQueue  chan Job      // Production Line
	readyPool chan chan Job // Channel of worker channels
}

var count = 0

func (l *Leader) dispatch() {
	for {
		select {
		case job := <-l.jobQueue: // chunk to process
			workerChannel := <-l.readyPool // worker X's channel
			workerChannel <- "map"
			workerChannel <- job // sending worker X a job
		}
	}
}

func handleConnection(c net.Conn) {

	//first: for loop waits around for a 'ready'
	//next: if it gets a ready, check if any chunks need processing
	//		if not, tell worker 'done' and close
	//next: for loop performs the handshake
	//next: for loop waits around for a string to be written by the worker
	//next: interpret the string (bufio readlines could be useful)
	//next: write the string into the global data structure

	//var workQueue chan chan WorkRequest
	for {
		netData, err := bufio.NewReader(c).ReadString('\n')
		if err != nil {
			log.Println(err) //prints to standard error
			return
		}
		fmt.Print(string(netData))

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

	freq := SafeMap{freqMap: make(map[string]int), m: &sync.RWMutex{}}
	//perform the file chunk division
	//create a global map data structure
	//loop which waits for N hosts to connect
	// --- N could be specified as a command line argument
	// --- pass a pointer to the buffer array (with mutex) to each connection
	// --- also need to pass a pointer to the global map (with mutex) to each connection
	// use sync.waitgroup to wait for all threads to complete
	// once all threads are complete, write the global map to a file
	// end

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
	fileSize := fileInfo.Size()             // get file size
	chunkSize := int(fileSize)/numHosts + 1 //size of chunk per each host
	fmt.Println("chunksize is", chunkSize)

	buffers := make([][][]byte, numHosts)
	for i := 0; i < numHosts; i++ {
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
		fmt.Println("bytes read:", bytesRead)
		buffers[i] = [][]byte{{0}, buff}
	}
	return &buffers
}
