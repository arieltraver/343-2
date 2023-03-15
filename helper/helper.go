package helper

import (
	"log"
	"net"
)

// error handling for readability
func CheckFatalErr(c net.Conn, err error) {
	if err != nil {
		c.Close()
		log.Fatal(err)
	}
}
