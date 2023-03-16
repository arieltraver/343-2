package main

import (
	"../helper"
	"bufio"
	"fmt"
	"io"
	"log"
	"math"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
)

var NUMCHUNKS int = 16 //number of chunks to divide file into

// a locked map structure, for the global result
type SafeMap struct {
	wordMap map[string]int
	lock    sync.Mutex
}

// a locked file, from which data will be sent to workers
type SafeFile struct {
	chunkSize int
	file      *os.File
	lock      sync.Mutex
}

/*
* Talks to a single remote worker. Upon receiving a "ready" keyword, if there
* are remaining file chunks, sends the worker a "map words" keyword and waits to
* receive "ok map" confirmation keyword, both through sendJobname().
* Upon receiving the worker's confirmation, grabs a file chunk and sends
* it to the worker. Returns if there are no file chunks left.
 */
func handleConnection(c net.Conn, globalMap *SafeMap, globalFile *SafeFile, wait *sync.WaitGroup, alldone chan int) {
	defer wait.Done()
	defer c.Close()
	globalFile.lock.Lock()
	chunkSize := globalFile.chunkSize
	globalFile.lock.Unlock()
	reader := bufio.NewReader(c) // passed between functions, reads from c
	ready := make(chan string, 2)
	for {
		select {
		case <-ready: // worker requests job
			bytes, err := grabMoreText(globalFile, alldone) // grab chunk
			helper.CheckFatalErrConn(c, err)
			if bytes == nil { // no more chunks to be read
				io.WriteString(c, "DONE\n")
				alldone <- 1
				c.Close()
				// main waits on each of these to reach this point
				// so it's important to stop making new connections once the file is complete
				return
			} else {
				sendJobName(c, chunkSize, reader)
				_, err := bufio.NewWriter(c).Write(bytes) // send chunk
				helper.CheckFatalErrConn(c, err)
				addResultToGlobal(c, globalMap, reader)
			}
		default:
			waitForReady(c, ready, reader)
		}
	}
}

/*
* Waits for a worker to request a job. Closes the connection and exits if the
* worker requests to stop. Sends "ready" to handleConnection() if the worker
* sends the ready signal.
 */
func waitForReady(c net.Conn, ready chan string, reader *bufio.Reader) {
	netData, err := reader.ReadString('\n')
	helper.CheckFatalErrConn(c, err)
	msg := strings.TrimSpace(strings.ToUpper(string(netData)))
	if msg == "STOP" {
		c.Close()
		log.Fatal("A worker has requested to STOP!")
	}
	if msg == "READY" {
		//	fmt.Println("A worker is ready!")
		ready <- "ready"
		return
	}
}

/*sends a string reading "map words" to a worker connected via net.Conn*/
func sendJobName(c net.Conn, chunkSize int, reader *bufio.Reader) {
	fmt.Println("sending job name!")
	s := "count words\n"
	_, err := io.WriteString(c, s)
	helper.CheckFatalErrConn(c, err)

	_, err2 := io.WriteString(c, strconv.Itoa(chunkSize)+"\n")
	helper.CheckFatalErrConn(c, err2)

	netData, err := reader.ReadString('\n')
	helper.CheckFatalErrConn(c, err)

	msg := strings.TrimSpace(strings.ToUpper(string(netData)))
	if msg == "STOP" {
		c.Close()
		log.Fatal("A worker has requested to STOP!")
	}
	if msg == "ok count words" {
		fmt.Println("Worker is okay with counting words!")
		return
	}
}

/*
Takes a string input from the worker and inputs results to global map data structure
*/
func addResultToGlobal(c net.Conn, globalMap *SafeMap, reader *bufio.Reader) {
	result, err := reader.ReadString('\n')
	helper.CheckFatalErrConn(c, err)
	reader2 := strings.NewReader(result)
	scanner := bufio.NewScanner(reader2)
	scanner.Split(bufio.ScanWords) // word:count divided by spaces
	for scanner.Scan() {
		wdAndCount := strings.Split(scanner.Text(), ":")
		if len(wdAndCount) != 2 {
			c.Close()
			log.Fatal("unexpected entry")
		}
		count, err := strconv.Atoi(wdAndCount[1])
		helper.CheckFatalErrConn(c, err)
		globalMap.lock.Lock()                     // acquire lock
		globalMap.wordMap[wdAndCount[0]] += count // add to the global map
		globalMap.lock.Unlock()                   // release lock
	}

}

func splitToChunks(fileName string, numChunks int) {
	fileToBeChunked := fileName

	file, err := os.Open(fileToBeChunked)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	defer file.Close()

	fileInfo, _ := file.Stat()
	var fileSize int64 = fileInfo.Size()
	var chunkSize = int(math.Ceil(float64(fileSize) / float64(numChunks)))

	for i := 0; i < numChunks-1; i++ {

		//partSize := int(math.Min(fileChunk, float64(fileSize-int64(i*fileChunk))))
		partBuffer := make([]byte, chunkSize)
		file.Read(partBuffer)

		// Add it to the array of byte slices
		fileChunks = append(fileChunks, FileChunkInfo{partBuffer, false})
	}
	// Read the last chunk
	partSize := int(math.Min(float64(chunkSize),
		float64(fileSize-int64((numChunks-1)*chunkSize))))
	partBuffer := make([]byte, partSize)
	file.Read(partBuffer)
}

/*Takes a locked file object and reads some bytes, returns the array*/
func grabMoreText(globalFile *SafeFile, alldone chan int) ([]byte, error) {
	globalFile.lock.Lock()
	file := globalFile.file
	chunkSize := globalFile.chunkSize

	//reader := bufio.ReadString(file)
	buff := make([]byte, chunkSize)
	bytesRead, err := file.Read(buff) // read the length of buffer from file
	helper.CheckFatalErr(err)
	if err != nil {
		if err == io.EOF {
			fmt.Println("reached end of file")
			globalFile.lock.Unlock()
			return nil, nil
		} else {
			log.Fatal(err)
		}
	}
	globalFile.lock.Unlock()
	fmt.Println("bytes read:", bytesRead)
	return buff, nil
}

/*Writes the final word count results to an output file.*/
func writeMapToFile(filename string, globalMap *SafeMap) {
	output, err := os.Create(filename)
	helper.CheckFatalErr(err)
	defer output.Close() // make sure file closes before return
	writer := bufio.NewWriter(output)
	globalMap.lock.Lock()
	words := helper.SortWords(globalMap.wordMap)

	for _, key := range words {
		str := key + " " + strconv.Itoa(globalMap.wordMap[key]) + "\n"
		_, err := writer.WriteString(str)
		helper.CheckFatalErr(err)
	}
	globalMap.lock.Unlock()
	writer.Flush()
}

/*
Waits for new connections on your port (specified by net.Listener)
You can have as many workers as you want
It gives jobs out to whatever worker is ready
*/
func waitOnConnections(listener net.Listener, globalMap *SafeMap, globalFile *SafeFile, wait *sync.WaitGroup, alldone chan int) {
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Println("failed connection")
		} else { //if one connection fails you can have more
			fmt.Println("new host joining:", conn.RemoteAddr())
			wait.Add(1)                                                     //add new routine to the waitgroup
			go handleConnection(conn, globalMap, globalFile, wait, alldone) // Each client served by a different routine
			//keep track of how many workers are connected
		}
	}
}

/* Creates a SafeFile struct from a directory with one file. Returns the SafeFile as well as the size of each chunk in bytes.
 */
func prepareFile(directory string, NUMCHUNKS int) *SafeFile {
	fps, err := os.ReadDir(directory)
	helper.CheckFatalErr((err))
	file, err := os.Open(directory + "/" + fps[0].Name())
	helper.CheckFatalErr((err))
	fileInfo, err := file.Stat() // get file stats
	helper.CheckFatalErr((err))
	fileSize := fileInfo.Size() // get file size
	//chunkSize := int(fileSize)/NUMCHUNKS + 1 //size of chunk per each host

	chunkSize := int(math.Ceil(float64(fileSize) / float64(NUMCHUNKS)))
	SafeFile := &SafeFile{chunkSize: chunkSize, file: file}
	return SafeFile
}

func main() {

	//initialize a global file --DONE tested
	//initialize a global hash map --DONE tested
	//loop which waits for new hosts to connect --DONE tested
	// -- pass a pointer to the global locked file  --DONE tested
	// --- also need to pass a pointer to the global map (with mutex) to each connection --DONE tested
	// use sync.waitgroup to wait for all threads to complete --DONE tested
	// once all threads are complete, write the global map to a file --DONE tested
	// end

	arguments := os.Args
	if len(arguments) <= 2 {
		fmt.Println("Usage: 'leader port directory'")
		return
	}
	PORT := ":" + arguments[1]
	listener, err := net.Listen("tcp4", PORT)
	helper.CheckFatalErr((err))
	defer listener.Close()
	fmt.Println("listening on port", arguments[1])

	directory := arguments[2]
	fmt.Println("directory:", directory)
	globalFile := prepareFile(directory, NUMCHUNKS) // create a filepointer to the one file in there
	globalMap := &SafeMap{                          //lock so one thread at a time may use it
		wordMap: make(map[string]int),
	}

	var wait sync.WaitGroup              // wait on all hosts to complete
	alldone := make(chan int, NUMCHUNKS) // check if done, with extra space

	go waitOnConnections(listener, globalMap, globalFile, &wait, alldone)

	for {
		select {
		case <-alldone:
			wait.Wait()
			writeMapToFile("../output/output.txt", globalMap)
			return
		}
	}
}
