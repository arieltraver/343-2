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
	//"math"
)

var count = 0

func handleConnection(c net.Conn, globalMap *LockedMap, wait *sync.WaitGroup, globalCount *LockedInt, allgood *chan int) {
	defer wait.Done()
	defer c.Close()
	addToCount(globalCount, 1)
	if checkCount(globalCount) >= 4 {
		*allgood <- 1
	}
	//first: for loop waits around for a 'ready'
	//next: if it gets a ready, check if any chunks need processing
	//		if not, tell worker 'done' and close
	//next: for loop performs the handshake
	//next: for loop waits around for a string to be written by the worker
	//next: interpret the string (bufio readlines could be useful)
	//next: write the string into the global data structure


	for {
		select {
		case <-*allgood:
			//send 'ready' to the host.
		default: //wait around and chill while the host waits for everybody to connect
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
	}

}

//a locked map structure, for the global result
type LockedMap struct {
	wordMap map[string] int;
	lock sync.Mutex
}

type LockedInt struct {
	count int
	lock sync.Mutex
}

func checkCount(globalCount *LockedInt) int {
	globalCount.lock.Lock()
	c := globalCount.count
	globalCount.lock.Unlock()
	return c
}
func addToCount(globalCount *LockedInt, diff int) {
	globalCount.lock.Lock()
	globalCount.count += diff
	globalCount.lock.Unlock()
}


//enter data into the global locked map structure
func enterData(routineMap map[string]int, globalMap *LockedMap) {
	globalMap.lock.Lock() //obtain the lock
	for word, count := range(routineMap) {
		globalMap.wordMap[word] = globalMap.wordMap[word] + count
	}
	globalMap.lock.Unlock() //release the lock
}

func main() {
	
	//perform the file chunk division --DONE
	//create a global map data structure --DONE
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
	totalMap := make(map[string] int)
	globalMap := &LockedMap{
		wordMap: totalMap,
	}
	globalCount := &LockedInt{
		count: 0,
	}

	var wait sync.WaitGroup //wait on all hosts to complete
	allgood := make(chan int, 1)

	for { // change to a select, change globalcount
		select {
		case <- allgood:
			//perform the handshake, progress to the next stage
		default:
			fmt.Println("workers connected:", checkCount(globalCount))
			//Stops if handleConnection() reads STOP
			conn, err := listener.Accept()
			if err != nil {
				log.Println("failed connection")
				return
			} else { //if one connection fails you can have more
				fmt.Println("new host joining:", conn.RemoteAddr())
				go handleConnection(conn, globalMap, &wait, globalCount, &allgood) // Each client served by a different goroutine
			}
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
	fileSize := fileInfo.Size() // get file size
	chunkSize := int(fileSize) / numHosts + 1 //size of chunk per each host
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
