package main

import (
	"fmt"
	"log"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-zookeeper/zk"
)

func init() {
	log.SetFlags(log.Ltime | log.Lshortfile)
}

var wg sync.WaitGroup

func main() {
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go worker(i)
	}
	wg.Wait()
}

const (
	PATH    = "/simple_lock"
	_PREFIX = "lock-"
	PREFIX  = PATH + "/" + _PREFIX
)

func worker(id int) {
	defer wg.Done()

	logger := log.New(log.Writer(), fmt.Sprintf("[Worker %d] ", id), log.Ltime|log.Llongfile)

	conn, _, err := zk.Connect([]string{"127.0.0.1:2181"}, time.Second)
	if err != nil {
		logger.Fatalf("Can't connect: %v", err)
	}
	defer conn.Close()

	if exist, _, _ := conn.Exists(PATH); !exist {
		conn.Create(PATH, []byte{}, 0, zk.WorldACL(zk.PermAll))
	}

	path, _ := conn.Create(PREFIX, []byte{}, zk.FlagEphemeral|zk.FlagSequence, zk.WorldACL(zk.PermAll))
	n, _ := strconv.Atoi(strings.TrimPrefix(path, PREFIX))
	logger.Printf("%v created! n = %v", path, n)

	for {
		children, _, _ := conn.Children(PATH) // children = ["lock-0000000023", "lock-0000000022", "lock-0000000021"]

		var prev int
		var prevPath string
		for _, child := range children {
			i, _ := strconv.Atoi(strings.TrimPrefix(child, _PREFIX))
			if prev < i && i < n {
				prev, prevPath = i, child
			}
		}
		if prevPath == "" {
			logger.Print("n is the lowest znode in C")
			break
		}

		logger.Printf("znode in C ordered just before n: %v", prevPath)

		exist, _, watch, _ := conn.ExistsW(PATH + "/" + prevPath)
		if exist {
			logger.Printf("Wait for %v", prevPath)
			<-watch
			logger.Print("Watch event")
		}
	}

	logger.Print("Acquire lock, do some work...")
	time.Sleep(time.Second * time.Duration(rand.Intn(5)))
}
