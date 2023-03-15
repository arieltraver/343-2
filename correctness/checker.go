package main

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

func main() {
	our_map := file_to_map("../output/output.txt")
	reliable_map := file_to_map("../output/reliableOutput.txt")

	for sk, sv := range our_map {
		count, ok := reliable_map[sk]
		if !ok {
			fmt.Println("FAIL: output.txt contains " + sk + ", but reliableOutput.txt doesn't")
			os.Exit(1)
		}
		if count != sv {
			fmt.Println("FAIL: output.txt has count " + fmt.Sprint(sk) + " for " + sk + ", but reliableOutput.txt has " + fmt.Sprint(count))
			os.Exit(1)
		}
	}

	fmt.Println("OK")
}

func file_to_map(file_name string) map[string]int {
	result_map := make(map[string]int)
	dat, err := os.ReadFile(file_name)
	if err != nil {
		fmt.Println("FAIL: " + file_name + " not found")
		os.Exit(1)
	}

	arr := strings.Fields(string(dat))

	for i := 0; i < len(arr); i += 2 {
		counter, err := strconv.Atoi(arr[i+1])
		if err != nil {
			fmt.Println("FAIL: Formatting error - " + arr[i+1] + " is not an integer")
			fmt.Println("counter: " + strconv.Itoa(counter))
			fmt.Println(file_name)
			os.Exit(1)
		}
		result_map[arr[i]] = counter
	}
	return result_map
}
