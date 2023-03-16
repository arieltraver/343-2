package main

import (
	"bufio"
	"fmt"
	"github.com/arieltraver/343-2/helper"
	"io"
	"log"
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
	reader    *bufio.Reader
}

// Talks to a single remote worker. Upon receiving a "ready" keyword, if there
// are remaining file chunks, sends the worker a "map words" keyword and waits to
// receive "ok map" confirmation keyword, both through sendJobname().
// Upon receiving the worker's confirmation, grabs a file chunk and sends
// it to the worker. If there are no file chunks left, communicates this
// through a channel, writes "DONE" keyword to workers, closes the connection,
// and returns.
func handleConnection(c net.Conn, globalMap *SafeMap, globalFile *SafeFile, wait *sync.WaitGroup, alldone chan int) {
	defer wait.Done()
	defer c.Close()
	globalFile.lock.Lock()
	globalFile.lock.Unlock()
	reader := bufio.NewReader(c) // passed between functions, reads from c
	ready := make(chan string, 2)
	for {
		select {
		case <-ready: // worker requests job
			bytes, err := grabMoreText(globalFile) // grab file chunk
			helper.CheckFatalErrConn(c, err)
			if bytes == nil { // no more chunks to be read
				io.WriteString(c, "DONE\n")
				c.Close()
				alldone <- 1
				// main waits on each of these to reach this point
				// so it's important to stop making new connections once the
				// file is complete
				return
			} else {
				chunkSize := len(bytes)
				sendJobName(c, chunkSize, reader)
				b := bufio.NewWriter(c)
				_, err := b.Write(bytes) // send chunk
				helper.CheckFatalErrConn(c, err)
				addResultToGlobal(c, globalMap, reader)
			}
		default:
			waitForReady(c, ready, reader)
		}
	}
}

// Waits for a worker to request a job. Closes the connection and exits if the
// worker requests to stop. Sends "ready" to handleConnection() if the worker
// sends the ready signal.
func waitForReady(c net.Conn, ready chan string, reader *bufio.Reader) {
	netData, err := reader.ReadString('\n')
	helper.CheckFatalErrConn(c, err)
	msg := strings.TrimSpace(strings.ToUpper(string(netData)))
	if msg == "STOP" {
		c.Close()
		log.Fatal("A worker has requested to STOP!")
	}
	if msg == "READY" {
		ready <- "ready"
		return
	}
}

// Sends a string reading "map words" to a worker connected via net.Conn.
// Exists if the worker sends a stop request and eturns if the worker responds
// with an "ok count words" confirmation.
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
	if msg == "OK COUNT WORDS" {
		fmt.Println("Worker is okay with counting words!")
		return
	}

}

// Given a string input from a worker, adds the mapping results from the string
// into the global map data structure.
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

// Reads a predetermined amount of bytes from a SafeFile and returns the
// corresponding byte array and the remainder of any words that were cut off.
func grabMoreText(globalFile *SafeFile) ([]byte, error) {
	globalFile.lock.Lock()
	chunkSize := globalFile.chunkSize
	file := globalFile.file
	buff := make([]byte, chunkSize)
	bytesRead, err := file.Read(buff) // read the length of buffer from file
	if err != nil {
		if err == io.EOF {
			fmt.Println("reached end of file")
			globalFile.lock.Unlock()
			return nil, nil
		} else {
			log.Fatal(err)
		}
	}
	extra := []byte(getStr(file)) // read till next space if present
	fmt.Println("bytes read:", bytesRead+len(extra))
	globalFile.lock.Unlock()
	chunk := append(buff, extra...)
	return chunk, nil
}

// Reads from file up to encountering whitespace. Used to account for words
// that may be cut off when reading by amount of bytes. Returns string
func getStr(file *os.File) string {
	extra := strings.Builder{}
	b := make([]byte, 1)
	for {
		read, err := file.Read(b)
		if err != nil && err != io.EOF {
			log.Println("while grabbing extra string:", err)
			return extra.String()
		} else if read == 0 || b[0] == ' ' {
			return extra.String()
		} else {
			extra.Grow(1)
			extra.WriteByte(b[0])
		}
	}
}

// Writes the final word count results from globalMap to an output file.
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

// Waits for new connections on port (specified by net.Listener). Serves each
// worker with a different goroutine.
func waitOnConnections(listener net.Listener, globalMap *SafeMap, globalFile *SafeFile, wait *sync.WaitGroup, alldone chan int) {
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Println("failed connection")
		} else { //if one connection fails you can have more
			fmt.Println("new host joining:", conn.RemoteAddr())
			wait.Add(1)                                                     // add new routine to the waitgroup
			go handleConnection(conn, globalMap, globalFile, wait, alldone) // each client served by a different routine
		}
	}
}

// Creates a SafeFile struct from a directory with one file. Returns the
// SafeFile as well as the size of each chunk in bytes.
func prepareFile(directory string, NUMCHUNKS int) *SafeFile {
	fps, err := os.ReadDir(directory)
	helper.CheckFatalErr((err))

	// assuming one file in directory
	file, err := os.Open(directory + "/" + fps[0].Name())
	helper.CheckFatalErr((err))

	fileInfo, err := file.Stat() // get file stats
	helper.CheckFatalErr((err))

	fileSize := fileInfo.Size()              // get file size in bytes
	chunkSize := int(fileSize)/NUMCHUNKS + 1 // size of chunk per host
	reader := bufio.NewReader(file)

	SafeFile := &SafeFile{chunkSize: chunkSize, file: file, reader: reader}
	return SafeFile
}

func main() {
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

	// initialize a global file
	globalFile := prepareFile(directory, NUMCHUNKS)
	// initialize a global map
	globalMap := &SafeMap{
		wordMap: make(map[string]int),
	}

	var wait sync.WaitGroup              // to wait on all hosts to complete
	alldone := make(chan int, NUMCHUNKS) // check if done, with extra space

	go waitOnConnections(listener, globalMap, globalFile, &wait, alldone)

	for {
		select {
		case <-alldone:
			wait.Wait() // waits for all workers to finish current jobs
			writeMapToFile("../output/output.txt", globalMap)
			return
		}
	}
}
