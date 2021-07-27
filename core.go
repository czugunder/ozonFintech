package main

import (
	"bufio"
	"fmt"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"
)

var waitGroup sync.WaitGroup

type pair struct {
	value interface{}
	ttl   int64
}

func (p pair) expired() bool {
	if p.ttl == 0 {
		return false
	} else {
		return time.Now().UnixNano() >= p.ttl
	}
}

type janitor struct {
	interval time.Duration
	running  bool
	stop     chan bool
}

func (jan *janitor) work(st *storage) {
	jan.running = true
	clock := time.NewTicker(jan.interval)
	for {
		select {
		case <-clock.C:
			st.cleanup()
		case <-jan.stop:
			clock.Stop()
			jan.running = false
			return
		}
	}
}

func (st *storage) stopJanitor() {
	st.janitor.stop <- true
}

func (st *storage) runJanitor() {
	go st.janitor.work(st)
}

type storage struct {
	pairs       map[string]pair // TODO make a point that it is impossible to make a byte slice a key, strings used instead
	mutex       sync.RWMutex
	defaultExp  time.Duration // if == 0 then ttl is off
	janitor     *janitor
	maxElements int
}

func (st *storage) init(defExp time.Duration, janInt time.Duration) {
	st.pairs = make(map[string]pair)
	st.defaultExp = defExp
	st.janitor = &janitor{
		interval: janInt,
		stop:     make(chan bool),
	}
}

func (st *storage) add(inKey string, inValue interface{}, inTTL time.Duration) {
	defer waitGroup.Done()
	var inExp int64
	if inTTL > 0 { //what if i want undeletable pair with default != 0
		inExp = time.Now().Add(inTTL).UnixNano()
	} else {
		inExp = time.Now().Add(st.defaultExp).UnixNano()
	}
	st.mutex.Lock()
	st.pairs[inKey] = pair{value: inValue, ttl: inExp}
	st.mutex.Unlock()
}

func (st *storage) find(inKey string, interfaceCh chan interface{}) {
	defer waitGroup.Done()
	var result interface{}
	st.mutex.RLock()
	if val, ok := st.pairs[inKey]; ok {
		if st.janitor.running == false || !val.expired() {
			result = val.value
		}
	}
	interfaceCh <- result
	st.mutex.RUnlock()
}

func (st *storage) del(inKey string, boolCh chan bool) {
	defer waitGroup.Done()
	var wasDeleted bool
	st.mutex.Lock()
	if _, ok := st.pairs[inKey]; ok {
		delete(st.pairs, inKey)
		wasDeleted = true
	}
	boolCh <- wasDeleted
	st.mutex.Unlock()

}

func (st *storage) flush() {
	st.mutex.Lock()
	st.pairs = make(map[string]pair)
	st.mutex.Unlock()
}

func (st *storage) cleanup() {
	st.mutex.Lock()
	for key, val := range st.pairs {
		if val.expired() {
			delete(st.pairs, key)
		}
	}
	st.mutex.Unlock()
}

func main() {
	var mainStorage storage
	janitorInterval, _ := time.ParseDuration("10s")
	mainStorage.init(0, janitorInterval)

	fmt.Println("Key - Value storage enabled!")
	directMode(&mainStorage)
}

func directMode(st *storage) {
	var command, resultString string
	var resultBool bool
	var addCommandRegex = regexp.MustCompile("^add\\s\\w+\\s\\w+\\s\\w+$")
	var findCommandRegex = regexp.MustCompile("^find\\s\\w+$")
	var delCommandRegex = regexp.MustCompile("^del\\s\\w+$")
	var janitorEnableCommandRegex = regexp.MustCompile("janitor\\s\\w+$")
	var janitorInervalCommandRegex = regexp.MustCompile("interval\\s\\w+$")
	var defaultTTLCommandRegex = regexp.MustCompile("ttl\\s\\w+$")
	var commandBuffer []string
	var resultInterface interface{}

	var interfaceChannel = make(chan interface{})
	var boolChannel = make(chan bool)
	reader := bufio.NewReader(os.Stdin)
	fmt.Println("CMD interface active!")
loop:
	for {
		fmt.Print("storage> ")
		command, _ = reader.ReadString('\n')
		command = command[:len(command)-1]
		switch {
		case command == "quit":
			close(interfaceChannel)
			close(boolChannel)
			fmt.Println("Stopped.")
			break loop
		case addCommandRegex.MatchString(command):
			commandBuffer = strings.Split(command, " ")
			buffer, err := time.ParseDuration(commandBuffer[3]) // can buffer and err be inited before for loop?
			if err != nil {
				fmt.Println("Input has bad TTL parameter, try in XhXmXs format.")
			} else {
				waitGroup.Add(1)
				go st.add(commandBuffer[1], commandBuffer[2], buffer)
				waitGroup.Wait()
				fmt.Printf("Key-Value pair {%s: %s} saved!\n", commandBuffer[1], commandBuffer[2])
			}
		case findCommandRegex.MatchString(command):
			commandBuffer = strings.Split(command, " ")
			waitGroup.Add(1)
			go st.find(commandBuffer[1], interfaceChannel)
			resultInterface = <-interfaceChannel
			waitGroup.Wait()
			resultString = fmt.Sprintf("%v", resultInterface)
			if resultInterface != nil {
				fmt.Printf("Result found. Value for \"%s\" key is \"%s\".\n", commandBuffer[1], resultString)
			} else {
				fmt.Printf("Nothing found on \"%s\" request.\n", commandBuffer[1])
			}
		case delCommandRegex.MatchString(command):
			commandBuffer = strings.Split(command, " ")
			waitGroup.Add(1)
			go st.del(commandBuffer[1], boolChannel)
			resultBool = <-boolChannel
			waitGroup.Wait()
			if resultBool == true {
				fmt.Printf("Key-Value pair with \"%s\" key was erased!\n", commandBuffer[1])
			} else {
				fmt.Printf("Key-Value pair with \"%s\" key was not found.\n", commandBuffer[1])
			}
		case janitorEnableCommandRegex.MatchString(command):
			commandBuffer = strings.Split(command, " ")
			if commandBuffer[1] == "on" {
				if st.janitor.running == false {
					st.runJanitor()
					fmt.Println("Janitor enabled!")
				} else {
					fmt.Println("Janitor has been already enabled.")
				}
			} else if commandBuffer[1] == "off" {
				if st.janitor.running == true {
					st.stopJanitor()
					fmt.Println("Janitor stopped!")
				} else {
					fmt.Println("Janitor has been already stopped.")
				}
			} else {
				fmt.Println("Incorrect janitor command.")
			}
		case janitorInervalCommandRegex.MatchString(command):
			commandBuffer = strings.Split(command, " ")
			buffer, err := time.ParseDuration(commandBuffer[1]) // can buffer and err be inited before for loop?
			if err != nil {
				fmt.Println("Input has bad janitor interval time parameter, try in XhXmXs format.")
			} else {
				st.janitor.interval = buffer
				fmt.Printf("Default janitor interval time is set to %s.", commandBuffer[1])
			}
		case defaultTTLCommandRegex.MatchString(command):
			commandBuffer = strings.Split(command, " ")
			buffer, err := time.ParseDuration(commandBuffer[1]) // can buffer and err be inited before for loop?
			if err != nil {
				fmt.Println("Input has bad TTL parameter, try in XhXmXs format.")
			} else {
				st.defaultExp = buffer
				fmt.Printf("Default TTL is set to %s.", commandBuffer[1])
			}
		default:
			fmt.Println("Bad command. Try again or type \"help\".")
		}
	}
}
