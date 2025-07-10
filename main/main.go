package main

import (
	"krpc"
	"log"
	"net"
	"sync"
	"time"
)

type Service int

type Args struct{ Num1, Num2 int }

func (f Service) Sum(args Args, reply *int) error {
	*reply = args.Num1 + args.Num2
	return nil
}

func start(addr chan string) {
	var s Service
	err := krpc.Register(&s)
	if err != nil {
		log.Println("register err:", err)
		return
	}
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

	time.Sleep(time.Second)
	// send request & receive response
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			args := &Args{Num1: i, Num2: i * i}
			var reply int
			if err := client.Call("Service.Sum", args, &reply); err != nil {
				log.Fatal("call service error:", err)
			}
			log.Printf("%d + %d = %d", args.Num1, args.Num2, reply)
		}(i)
	}
	wg.Wait()
}
