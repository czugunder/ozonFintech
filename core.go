package main

import (
	"bufio"
	"fmt"
	"os"
	"regexp"
	"strings"
)

type pair struct {
	value []byte
	size  int
	ttl   int
}

type storage struct {
	pairs    map[string]pair // TODO make a point that it is impossible to make a byte slice a key, strings used instead
	quantity int
	size     int
}

func (st *storage) init() {
	st.pairs = make(map[string]pair)
}

func (st *storage) add(inKey, inValue []byte) {
	st.pairs[bytesToStr(inKey)] = pair{value: inValue}
}

func (st *storage) find(inKey []byte) (result []byte) {
	if val, ok := st.pairs[bytesToStr(inKey)]; ok {
		result = val.value
	}
	return
}

func (st *storage) del(inKey []byte) {
	delete(st.pairs, bytesToStr(inKey))
}

func main() {
	var mainStorage storage
	mainStorage.init()
	fmt.Println("Key - Value storage enabled!")
	directMode(&mainStorage)
}

func strToBytes(str string) []byte {
	return []byte(str)
}

func bytesToStr(bytes []byte) string {
	return string(bytes)
}

func directMode(st *storage) {
	var command, resultString string
	var addCommandRegex = regexp.MustCompile("^add\\s\\w+\\s\\w+$")
	var findCommandRegex = regexp.MustCompile("^find\\s\\w+$")
	var delCommandRegex = regexp.MustCompile("^del\\s\\w+$")
	var commandBuffer []string
	var resultBytes []byte
	reader := bufio.NewReader(os.Stdin)
	fmt.Println("CMD interface active!")
loop:
	for {
		fmt.Print("storage> ")
		command, _ = reader.ReadString('\n')
		command = command[:len(command)-1]
		switch {
		case command == "quit":
			fmt.Println("Stopped.")
			break loop
		case addCommandRegex.MatchString(command):
			commandBuffer = strings.Split(command, " ")
			st.add(strToBytes(commandBuffer[1]), strToBytes(commandBuffer[2]))
			fmt.Printf("Key-Value pair {%s: %s} saved!\n", commandBuffer[1], commandBuffer[2])
		case findCommandRegex.MatchString(command):
			commandBuffer = strings.Split(command, " ")
			resultBytes = st.find(strToBytes(commandBuffer[1]))
			resultString = bytesToStr(resultBytes)
			if len(resultString) > 0 {
				fmt.Printf("Result found. Value for \"%s\" key is \"%s\".\n", commandBuffer[1], resultString)
			} else {
				fmt.Printf("Nothing found on \"%s\" request.\n", commandBuffer[1])
			}
		case delCommandRegex.MatchString(command):
			commandBuffer = strings.Split(command, " ")
			st.del(strToBytes(commandBuffer[1]))
			fmt.Printf("Key-Value pair with \"%s\" key does not exist anymore!\n", commandBuffer[1])
		default:
			fmt.Println("Bad command. Try again or type \"help\".")
		}
	}
}
