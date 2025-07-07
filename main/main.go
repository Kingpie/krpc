package main

import (
	"fmt"
	"krpc"
	"log"
	"net"
	"sync"
	"time"
)

func start(addr chan string) {
	l, err := net.Listen("tcp", ":0")
	if err != nil {
		log.Fatal("listen error:", err)
		return
	}
	log.Println("start rpc server on", l.Addr())
	addr <- l.Addr().String()
	krpc.Accept(l)
}

func main() {
	addr := make(chan string)
	go start(addr)
	client, _ := krpc.Dial("tcp", <-addr)
	defer func() { _ = client.Close() }()

	time.Sleep(time.Second * 2)
	// send request & receive response
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			args := fmt.Sprintf("krpc req %d", i)
			var reply string
			if err := client.Call("Service.Test", args, &reply); err != nil {
				log.Fatal("call service error:", err)
			}
			log.Println("reply:", reply)
		}(i)
	}
	wg.Wait()
}
