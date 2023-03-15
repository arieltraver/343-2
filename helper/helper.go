package helper

import (
	"log"
	"net"
	"sort"
)

/**
* Given an error object, checks whether the object is null and if not, logs
* the error and exits.
**/
func CheckFatalErr(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

/**
* Given a connection and an error object, checks whether the object is null and
* if not, closes the connection, logs the error, and exits.
**/
func CheckFatalErrConn(c net.Conn, err error) {
	if err != nil {
		c.Close()
		log.Fatal(err)
	}
}

/**
* Given a string:int hashmap, creates a string array of the hashmap keys and
* returns the sorted array.
**/
func SortWords(freq map[string]int) []string {
	words := make([]string, len(freq))
	i := 0
	for key := range freq {
		words[i] = key
		i += 1
	}
	sort.Strings(words)
	return words
}
