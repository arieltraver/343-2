//used tutorial here: https://www.linode.com/docs/guides/developing-udp-and-tcp-clients-and-servers-in-go/

package main
import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strings"
	"log"
)
func main() {
	args := os.Args
	if len(args) <= 1 {
		log.Fatal("please provide host:port to connect to")
	}
	conn := args[1]
	c, err := net.Dial("tcp", conn) //connect to host:port
	if err != nil {
		log.Fatal(err)
	}
	for {
		reader := bufio.NewReader(os.Stdin) //read input
		fmt.Print(">>> ")
		txt, err := reader.ReadString('\n') //take in what is after the newline
		if err != nil {
			log.Fatal(err)
		}
		fmt.Fprintf(c, txt + "\n") //print connection text and your text
		
	}

}