package main

import (
	"bufio"
	"encoding/gob"
	"fmt"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"
)

var waitGroup sync.WaitGroup

type pair struct {
	Value interface{}
	TTL   int64
}

func (p pair) expired() bool {
	if p.TTL == 0 {
		return false
	} else {
		return time.Now().UnixNano() >= p.TTL
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
	defaultExp  time.Duration
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
	var inExp = time.Now().Add(inTTL).UnixNano()
	st.mutex.Lock()
	st.pairs[inKey] = pair{Value: inValue, TTL: inExp}
	st.mutex.Unlock()
}

func (st *storage) find(inKey string, interfaceCh chan interface{}) {
	defer waitGroup.Done()
	var result interface{}
	st.mutex.RLock()
	if val, ok := st.pairs[inKey]; ok {
		if st.janitor.running == false || !val.expired() {
			result = val.Value
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

func (st *storage) save(filepath string) error {
	file, err := os.Create(filepath)
	if err != nil {
		return err
	}
	encoder := gob.NewEncoder(file)
	st.mutex.RLock()
	for _, val := range st.pairs {
		gob.Register(val.Value)
	}
	err = encoder.Encode(&st.pairs)
	st.mutex.RUnlock()
	if err != nil {
		file.Close()
		return err
	} else {
		return file.Close()
	}
}

func (st *storage) load(filepath string) error {
	file, err := os.Open(filepath)
	if err != nil {
		return err
	}
	decoder := gob.NewDecoder(file)
	loadedPairs := map[string]pair{}
	err = decoder.Decode(&loadedPairs)
	if err != nil {
		return err
	} else {
		st.mutex.RLock()
		for key, val := range loadedPairs {
			p, exist := st.pairs[key]
			if exist == false || p.expired() {
				st.pairs[key] = val
			}
		}
		st.mutex.RUnlock()
	}
	return file.Close()
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
	var addDefaultTTLCommandRegex = regexp.MustCompile("^add\\s\\w+\\s\\w+$")
	var findCommandRegex = regexp.MustCompile("^find\\s\\w+$")
	var delCommandRegex = regexp.MustCompile("^del\\s\\w+$")
	var janitorEnableCommandRegex = regexp.MustCompile("janitor\\s\\w+$")
	var janitorIntervalCommandRegex = regexp.MustCompile("interval\\s\\w+$")
	var defaultTTLCommandRegex = regexp.MustCompile("ttl\\s\\w+$")
	var saveStorageCommandRegex = regexp.MustCompile("save\\s\\w+$")
	var loadStorageCommandRegex = regexp.MustCompile("load\\s\\w+$")
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
		case addDefaultTTLCommandRegex.MatchString(command):
			commandBuffer = strings.Split(command, " ")
			waitGroup.Add(1)
			go st.add(commandBuffer[1], commandBuffer[2], st.defaultExp)
			waitGroup.Wait()
			fmt.Printf("Key-Value pair {%s: %s} saved!\n", commandBuffer[1], commandBuffer[2])
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
		case janitorIntervalCommandRegex.MatchString(command):
			commandBuffer = strings.Split(command, " ")
			buffer, err := time.ParseDuration(commandBuffer[1]) // can buffer and err be inited before for loop?
			if err != nil {
				fmt.Println("Input has bad janitor interval time parameter, try in XhXmXs format.")
			} else {
				st.janitor.interval = buffer
				fmt.Printf("Default janitor interval time is set to %s.\n", commandBuffer[1])
			}
		case defaultTTLCommandRegex.MatchString(command):
			commandBuffer = strings.Split(command, " ")
			buffer, err := time.ParseDuration(commandBuffer[1]) // can buffer and err be inited before for loop?
			if err != nil {
				fmt.Println("Input has bad TTL parameter, try in XhXmXs format.")
			} else {
				st.defaultExp = buffer
				fmt.Printf("Default TTL is set to %s.\n", commandBuffer[1])
			}
		case saveStorageCommandRegex.MatchString(command):
			commandBuffer = strings.Split(command, " ")
			err := st.save(commandBuffer[1])
			if err != nil {
				fmt.Printf("Error occured: %s", err)
			} else {
				fmt.Printf("Storage saving process went well. Image was saved to \"%s\".\n", commandBuffer[1])
			}
		case loadStorageCommandRegex.MatchString(command):
			commandBuffer = strings.Split(command, " ")
			err := st.load(commandBuffer[1])
			if err != nil {
				fmt.Printf("Error occured: %s", err)
			} else {
				fmt.Printf("Storage loaded from image located in \"%s\".\n", commandBuffer[1])
			}
		default:
			fmt.Println("Bad command. Try again or type \"help\".")
		}
	}
}
