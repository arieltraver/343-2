package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"os"
	"io"
	"strconv"
	"strings"
	"sync"
	"bytes"
)

// Talks to a single remote worker.
func handleConnection(c net.Conn, globalMap *LockedMap, globalCount *LockedInt, globalFile *LockedFile, wait *sync.WaitGroup) {
	defer wait.Done()
	defer c.Close()
	globalFile.lock.Lock()
	chunkSize := globalFile.chunkSize
	globalFile.lock.Unlock()
	ready := make(chan string, 2)
	for {
		select {
		case <-ready:
			sendJobName(c, chunkSize)
			bytes, err := grabMoreText(globalFile)
			if err != nil {
				c.Close()
				log.Fatal(err)
			} else if bytes == nil && err == nil {
				//there are no more chunks to be read. end this routine
				//main waits on each of these to reach this point
				//so it's important to stop making new connections once the file is complete
				fmt.Fprint(c, "done")
				return
			} else {
				_, err := bufio.NewWriter(c).Write(bytes)
				if err != nil {
					c.Close()
					log.Fatal(err)
				}
				addResultToGlobal(c, globalMap)
			}
		default:
			waitForReady(c, ready)
		}
	}
}

//waits around for a worker to send a "ready" signal
func waitForReady(c net.Conn, ready chan string) {
	fmt.Println("waiting for ready")
	netData, err := bufio.NewReader(c).ReadString('\n') 
	if err != nil {
		c.Close()
		log.Fatal("Reading input has failed...\n", err)
	}
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
func sendJobName(c net.Conn, chunkSize int) {
	fmt.Println("sending job name!")
	s := "count words\n"
	_, err := io.WriteString(c, s)
	if err != nil {
		c.Close()
		log.Fatal(err)
	}
	_, err2 := io.WriteString(c, strconv.Itoa(chunkSize) + "\n")
	if err2 != nil {
		c.Close()
		log.Fatal(err2)
	}
	netData, err := bufio.NewReader(c).ReadString('\n')
	if err != nil {
		c.Close()
		log.Fatal(err)
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

func addResultToGlobal(c net.Conn, globalMap *LockedMap) {
	fmt.Println("waiting to receive result")
	b := make([]byte, 413571) //change this number, get it from conn
	bytesRead, err := bufio.NewReader(c).Read(b) //read server's message into bytes array
	if err != nil {
		c.Close()
		log.Fatal(err)
	}
	fmt.Println("bytes read from result:", bytesRead)
	byteReader := bytes.NewReader(b)
	scanner := bufio.NewScanner(byteReader)
	scanner.Split(bufio.ScanWords) //word:count divided by spaces
	globalMap.lock.Lock() //acquire lock
	for scanner.Scan() {
		wdcount := scanner.Text()
		fmt.Println(wdcount)
		wdAndCount := strings.Split(wdcount, ":")
		word := wdAndCount[0]
		count, err := strconv.Atoi(wdAndCount[1]) //format is "word:count word2:count2"
		if err != nil {
			c.Close()
			log.Fatal(err)
		}
		globalMap.wordMap[word] += count //add to the global map
	}
	globalMap.lock.Unlock() //release lock
}

/*Takes a locked file object and reads some bytes, returns the array*/
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
	wordMap map[string] int;
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
	words := globalMap.wordMap
	for word, count := range(routineMap) {
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
	for key, count := range(counts) {
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
	numChunks := 16 //for this assignment
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
	totalMap := make(map[string] int) //to be filled
	globalMap := &LockedMap{ //lock so one thread at a time may use it
		wordMap: totalMap,
	}
	//is it better to do this or to use a channel of separate ones? uncertain...
	//counts the number of workers online
	globalCount := &LockedInt{
		count: 0,
	}
	
	var wait sync.WaitGroup //wait on all hosts to complete
	alldone := make(chan int, numChunks) //for use by the routine that is making new connections

	waitOnConnections(listener, globalMap, globalCount, globalFile, &wait, &alldone)

	wait.Wait() //wait for all threads to finish

	//write the global map to a file
	globalMap.lock.Lock()
	writeMapToFile("output.txt", globalMap.wordMap)
	fmt.Println("all done folks")
	globalMap.lock.Unlock()
	return
}

/* waits for new connections on your port (specified by net.Listener)
ou can have as many workers as you want
however, once alldone channel is filled, then it will only accept maximum 1 new connection*
this is more of an edge case since we will only be testing with four
---> logic: every time the for loop runs it sees if alldone has been filled yet
---> so if alldone is filled while it's waiting, then it technically could accept 1 more
*/
func waitOnConnections(listener net.Listener, globalMap *LockedMap, globalCount *LockedInt, globalFile *LockedFile, wait *sync.WaitGroup, alldone *chan int) {
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
				go handleConnection(conn, globalMap, globalCount, globalFile, wait) // Each client served by a different routine
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

	fileInfo, err := file.Stat() // get file stats
	if err != nil {
		log.Fatal(err)
	}
	fileSize := fileInfo.Size() // get file size
	chunkSize := int(fileSize) / numChunks + 1 //size of chunk per each host

	lockedFile := &LockedFile{chunkSize: chunkSize, file: file}
	return lockedFile
}