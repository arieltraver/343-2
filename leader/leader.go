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

/* Talks to a single remote worker.
*/
func handleConnection(c net.Conn, globalMap *LockedMap, globalCount *LockedInt, globalFile *LockedFile, wait *sync.WaitGroup, allgood *chan int) {
	defer wait.Done()
	defer c.Close()
	ready := make(chan string)
	//first: for loop waits around for a 'ready' --DONE
	//next: if it gets a ready, check if any chunks need processing
	//		if not, tell worker 'done' and close
	//next: for loop performs the handshake
	//next: for loop waits around for a string to be written by the worker
	//next: interpret the string (bufio readlines could be useful)
	//next: write the string into the global data structure
	for {
		select {
		case <-ready:
			sendJobName(c)
			bytes, err := grabMoreText(globalFile)
			if err != nil {
				c.Close()
				log.Fatal(err)
			} else if bytes == nil && err == nil {
				fmt.Fprint(c, "done")
				*allgood <- 1 //let the main process know everybody is finished.
				return
			} else {
				_, err := bufio.NewWriter(c).Write(bytes)
				if err != nil {
					c.Close()
					log.Fatal(err)
				}
			}
		default:
			waitForReady(c, &ready)
		}
	}
}

func waitForReady(c net.Conn, ready *chan string) {
	for {
		netData, err := bufio.NewReader(c).ReadString('\n') 
		if err != nil {
			c.Close()
			log.Fatal("Reading input has failed...")
		}
		fmt.Print(string(netData))
		temp := strings.TrimSpace(strings.ToUpper(string(netData)))
		if temp == "STOP" {
			c.Close()
			log.Fatal("A worker has requested to STOP!")
		}
		if temp == "READY" {
			fmt.Println("A worker is ready!")
			*ready <- "ready"
			return
		}
	}
}

func sendJobName(c net.Conn) {
	for {
		_, err := bufio.NewWriter(c).WriteString("map words")
		if err != nil {
			c.Close()
			log.Fatal("Writing to worker has failed")
		}
		netData, err := bufio.NewReader(c).ReadString('\n')
		if err != nil {
			c.Close()
			log.Fatal("Reading input has failed...")
		}
		fmt.Print(string(netData))
		temp := strings.TrimSpace(strings.ToUpper(string(netData)))
		if temp == "STOP" {
			c.Close()
			log.Fatal("A worker has requested to STOP!")
		}
		if temp == "ok count words" {
			fmt.Println("Worker is okay with counting words!")
			return
		}
	}
}

func grabMoreText(globalFile *LockedFile) ([]byte, error) {
	globalFile.lock.Lock()
	file := globalFile.file
	chunkSize := globalFile.chunkSize
	buff := make([]byte, chunkSize)
	bytesRead, err := file.Read(buff) //read the length of buffer from file
	if err != nil {
		if err == io.EOF {
			fmt.Println("reached end of file")
			return nil, nil
		} else {
			log.Fatal(err)
		}
	}
	fmt.Println("bytes read:", bytesRead)
	globalFile.lock.Unlock()
	return buff, nil
}

//a locked map structure, for the global result
type LockedMap struct {
	wordMap *map[string] int;
	lock sync.Mutex
}

//a locked int to keep track of how many workers are connected
type LockedInt struct {
	count int
	lock sync.Mutex
}
//a locked file, from which data will be sent to workers
type LockedFile struct {
	chunkSize int
	file *os.File
	lock sync.Mutex
}

//checks a locked int and returns the value
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
	words := *globalMap.wordMap
	for word, count := range(routineMap) {
		words[word] = words[word] + count
	}
	globalMap.lock.Unlock() //release the lock
}

func writeMapToFile(filename string, counts *map[string]int) error {
	output, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("error creating output file")
	}
	defer output.Close() //make sure file closes before return.
	writer := bufio.NewWriter(output)
	total := 0
	for key, count := range(*counts) {
		str := key + " " + strconv.Itoa(count) + "\n"
		_, err := writer.WriteString(str)
		if err != nil {
			return fmt.Errorf("error writing to output file")
		}
		writer.Flush()
		total += 1
	}
	return nil
}

func main() {
	
	//initialize a global file --DONE untested
	//initialize a global hash map --DONE untested
	//loop which waits for N hosts to connect -- DONE untested
	// -- pass a pointer to the global locked file  --DONE unteseted
	// --- also need to pass a pointer to the global map (with mutex) to each connection --DONE untested
	// use sync.waitgroup to wait for all threads to complete --DONE untested
	// once all threads are complete, write the global map to a file
	// end

	
	arguments := os.Args
	if len(arguments) <= 1 {
		fmt.Println("Usage: 'leader port directory'")
		return
	}
	numChunks := 4 //for this assignment
	PORT := ":" + arguments[1]
	listener, err := net.Listen("tcp4", PORT)
	if err != nil {
		log.Fatal(err)
		return
	}
	defer listener.Close()
	fmt.Println("listening on port", arguments[1])

	directory := arguments[2]
	fmt.Println("directory:", directory)
	globalFile := prepareFile(directory, numChunks)
	totalMap := make(map[string] int)
	globalMap := &LockedMap{
		wordMap: &totalMap,
	}
	//counts the number of threads
	globalCount := &LockedInt{
		count: 0,
	}
	//counts the number of finished writes
	//when this hits 0, stop accepting new connections
	
	var wait sync.WaitGroup //wait on all hosts to complete
	allgood := make(chan int, numChunks)
	alldone := make(chan int, numChunks) //for use by the routine that is making new connections

	//separate thread lets new listeners in
	go waitOnConnections(listener, globalMap, globalCount, globalFile, &wait, &allgood, &alldone)

	for { // change to a select, change globalcount
		select {
		case <- allgood: //blocks till everybody is done
			wait.Wait() //wait for all threads to finish
			globalMap.lock.Lock()
			hashmap := globalMap.wordMap
			writeMapToFile("output.txt", hashmap)
			fmt.Println("all done folks")
			globalMap.lock.Unlock()
			return
		}
	}
}

func waitOnConnections(listener net.Listener, globalMap *LockedMap, globalCount *LockedInt, globalFile *LockedFile, wait *sync.WaitGroup, allgood *chan int, alldone *chan int) {
	for {
		select {
		case <- *alldone: //we are finished with the overall task
			return //dont accept new connections
		default:
			fmt.Println("workers connected:", checkCount(globalCount))
			conn, err := listener.Accept()
			if err != nil {
				log.Println("failed connection")
				return
			} else { //if one connection fails you can have more
				fmt.Println("new host joining:", conn.RemoteAddr())
				wait.Add(1) //add new routine to the waitgroup
				go handleConnection(conn, globalMap, globalCount, globalFile, wait, allgood) // Each client served by a different routine
				addToCount(globalCount, 1) //keep track of how many workers are connected
			}
		}
	}
}

/* Creates a LockedFile struct from a directory with one file. Returns the LockedFile as well as the size of each chunk in bytes.
*/
func prepareFile(directory string, numChunks int) *LockedFile {
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
	chunkSize := int(fileSize) / numChunks + 1 //size of chunk per each host

	lockFile := &LockedFile{chunkSize: chunkSize, file: file}
	return lockFile
}