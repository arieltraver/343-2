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
	//"bytes"
)

var numChunks int = 16

func checkFatalErr(c net.Conn, err error) {
	if err != nil {
		c.Close()
		log.Fatal(err)
	}
}

/**
Talks to a single remote worker. Upon receiving a "ready" keyword, if there
are remaining file chunks, sends the worker a "map words" keyword and waits to
receive "ok map" confirmation keyword, both through sendJobname().
Upon receiving the worker's confirmation, grabs a file chunk and sends
it to the worker. Returns if there are no file chunks left.
**/
func handleConnection(c net.Conn, globalMap *LockedMap, globalCount *LockedInt, globalFile *LockedFile, wait *sync.WaitGroup, alldone chan int) {
	defer wait.Done()
	defer c.Close()
	globalFile.lock.Lock()
	chunkSize := globalFile.chunkSize
	globalFile.lock.Unlock()
	reader := bufio.NewReader(c)
	ready := make(chan string, 2)
	for {
		select {
		case <-ready:
			bytes, err := grabMoreText(globalFile, alldone)
			if err != nil {
				c.Close()
				log.Fatal(err)
			} else if bytes == nil {
				io.WriteString(c, "DONE\n")
				c.Close()
				alldone <- 1
				//there are no more chunks to be read. end this routine
				//main waits on each of these to reach this point
				//so it's important to stop making new connections once the file is complete
				return
			} else {
				sendJobName(c, chunkSize, reader)
				_, err := bufio.NewWriter(c).Write(bytes)
				checkFatalErr(c, err)
				addResultToGlobal(c, globalMap, reader)
			}
		default:
			waitForReady(c, ready, reader)
		}
	}
}

//waits around for a worker to send a "ready" signal
func waitForReady(c net.Conn, ready chan string, reader *bufio.Reader) {
	fmt.Println("waiting for ready")
	netData, err := reader.ReadString('\n')
	checkFatalErr(c, err)
	fmt.Println(string(netData))
	temp := strings.TrimSpace(strings.ToUpper(string(netData)))
	if temp == "STOP" {
		c.Close()
		log.Fatal("A worker has requested to STOP!")
	}
	if temp == "READY" {
		fmt.Println("A worker is ready!")
		ready <- "ready"
		return
	}
}

/*sends a string reading "map words" to a worker connected via net.Conn*/
func sendJobName(c net.Conn, chunkSize int, reader *bufio.Reader) {
	fmt.Println("sending job name!")
	s := "count words\n"
	_, err := io.WriteString(c, s)
	checkFatalErr(c, err)
	_, err2 := io.WriteString(c, strconv.Itoa(chunkSize) + "\n")
	checkFatalErr(c, err2)
	netData, err := reader.ReadString('\n')
	checkFatalErr(c, err)
	fmt.Print(string(netData))
	temp := strings.TrimSpace(strings.ToUpper(string(netData)))
	if temp == "STOP" {
		c.Close()
		log.Fatal("A worker has requested to STOP!")
	}
	if temp == "ok count words" { // what if temp equals something other than these two options?
		fmt.Println("Worker is okay with counting words!")
		return
	}
}

func addResultToGlobal(c net.Conn, globalMap *LockedMap, reader *bufio.Reader) {
	result, err := reader.ReadString('\n')
	checkFatalErr(c, err)
	reader2 := strings.NewReader(result)
	scanner := bufio.NewScanner(reader2)
	scanner.Split(bufio.ScanWords) //word:count divided by spaces
	globalMap.lock.Lock()          //acquire lock
	for scanner.Scan() {
		wdcount := scanner.Text()
		fmt.Println(wdcount)
		wdAndCount := strings.Split(wdcount, ":")
		if len(wdAndCount) != 2 {
			c.Close()
			log.Fatal("unexpected entry")
		}
		word := wdAndCount[0]
		count, err := strconv.Atoi(wdAndCount[1]) //format is "word:count word2:count2"
		checkFatalErr(c, err)
		globalMap.wordMap[word] += count //add to the global map
	}
	globalMap.lock.Unlock() //release lock
}

/*Takes a locked file object and reads some bytes, returns the array*/
func grabMoreText(globalFile *LockedFile, alldone chan int) ([]byte, error) {
	globalFile.lock.Lock()
	file := globalFile.file
	chunkSize := globalFile.chunkSize
	buff := make([]byte, chunkSize)
	bytesRead, err := file.Read(buff) //read the length of buffer from file
	if err != nil {
		if err == io.EOF {
			fmt.Println("reached end of file")
			globalFile.lock.Unlock()
			return nil, nil
		} else {
			log.Fatal(err)
		}
	}
	fmt.Println("bytes read:", bytesRead)
	globalFile.lock.Unlock()
	return buff, nil
}

// a locked map structure, for the global result
type LockedMap struct {
	wordMap map[string]int
	lock    sync.Mutex
}

// a locked int to keep track of how many workers are connected
type LockedInt struct {
	count int
	lock  sync.Mutex
}

// a locked file, from which data will be sent to workers
type LockedFile struct {
	chunkSize int
	file      *os.File
	lock      sync.Mutex
}

// checks a locked int and returns the value
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

// enter data into the global locked map structure
func enterData(routineMap map[string]int, globalMap *LockedMap) {
	globalMap.lock.Lock() //obtain the lock
	words := globalMap.wordMap
	for word, count := range routineMap {
		words[word] = words[word] + count
	}
	globalMap.lock.Unlock() //release the lock
}

// Writes the final word count results to an output file.
func writeMapToFile(filename string, counts map[string]int) error {
	output, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("error creating output file")
	}
	defer output.Close() //make sure file closes before return.
	writer := bufio.NewWriter(output)
	for key, count := range counts {
		str := key + " " + strconv.Itoa(count) + "\n"
		_, err := writer.WriteString(str)
		if err != nil {
			return fmt.Errorf("error writing to output file")
		}
		writer.Flush()
	}
	return nil
}

func main() {

	//initialize a global file --DONE untested
	//initialize a global hash map --DONE untested
	//loop which waits for new hosts to connect --DONE TEST
	// -- pass a pointer to the global locked file  --DONE TESTED
	// --- also need to pass a pointer to the global map (with mutex) to each connection --DONE TESTED
	// use sync.waitgroup to wait for all threads to complete --DONE untested
	// once all threads are complete, write the global map to a file --DONE untested
	// end

	arguments := os.Args
	if len(arguments) <= 2 {
		fmt.Println("Usage: 'leader port directory'")
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

	directory := arguments[2]
	fmt.Println("directory:", directory)
	globalFile := prepareFile(directory, numChunks) //create a filepointer to the one file in there
	totalMap := make(map[string]int)                //to be filled
	globalMap := &LockedMap{                        //lock so one thread at a time may use it
		wordMap: totalMap,
	}
	//is it better to do this or to use a channel of separate ones? uncertain...
	//counts the number of workers online
	globalCount := &LockedInt{
		count: 0,
	}
<<<<<<< HEAD
	
	var wait sync.WaitGroup //wait on all hosts to complete
	alldone := make(chan int, numChunks) //check if done, with extra space
=======

	var wait sync.WaitGroup              //wait on all hosts to complete
	alldone := make(chan int, numChunks) //for use by the routine that is making new connections
>>>>>>> dfa148d5131b6d2c577a51d01daae24c5e59cdf1

	go waitOnConnections(listener, globalMap, globalCount, globalFile, &wait, alldone)

	for {
		select {
		case <- alldone:
			wait.Wait()
			globalMap.lock.Lock()
			writeMapToFile("output.txt", globalMap.wordMap)
			fmt.Println("all done folks")
			globalMap.lock.Unlock()
			return
		}
	}
}

/*
Waits for new connections on your port (specified by net.Listener)
You can have as many workers as you want
It gives jobs out to whatever worker is ready
*/
func waitOnConnections(listener net.Listener, globalMap *LockedMap, globalCount *LockedInt, globalFile *LockedFile, wait *sync.WaitGroup, alldone chan int) {
	for {
		fmt.Println("workers connected:", checkCount(globalCount))
		conn, err := listener.Accept()
		if err != nil {
			log.Println("failed connection")
			return
		} else { //if one connection fails you can have more
			fmt.Println("new host joining:", conn.RemoteAddr())
			wait.Add(1) //add new routine to the waitgroup
			go handleConnection(conn, globalMap, globalCount, globalFile, wait, alldone) // Each client served by a different routine
			addToCount(globalCount, 1) //keep track of how many workers are connected
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

	fileInfo, err := file.Stat() // get file stats
	if err != nil {
		log.Fatal(err)
	}
	fileSize := fileInfo.Size()              // get file size
	chunkSize := int(fileSize)/numChunks + 1 //size of chunk per each host

	lockedFile := &LockedFile{chunkSize: chunkSize, file: file}
	return lockedFile
}
