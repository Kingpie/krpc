package main

import (
	"context"
	"krpc"
	"log"
	"net"
	"net/http"
	"sync"
)

type Service int

type Args struct{ Num1, Num2 int }

func (f Service) Minus(args Args, reply *int) error {
	*reply = args.Num1 - args.Num2
	return nil
}

func start(addr chan string) {
	var s Service
	err := krpc.Register(&s)
	if err != nil {
		log.Println("register err:", err)
		return
	}
	l, err := net.Listen("tcp", ":9527")
	if err != nil {
		log.Fatal("listen error:", err)
		return
	}
	krpc.HandleHTTP()
	log.Println("start rpc server on", l.Addr())
	addr <- l.Addr().String()
	_ = http.Serve(l, nil)
}

func call(addrChan chan string) {
	client, _ := krpc.DialHTTP("tcp", <-addrChan)
	defer func() { _ = client.Close() }()

	// send request & receive response
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			args := &Args{Num1: i, Num2: i * i}
			var reply int
			if err := client.Call(context.Background(), "Service.Minus", args, &reply); err != nil {
				log.Fatal("call Service.Minus error:", err)
			}
			log.Printf("%d - %d = %d", args.Num1, args.Num2, reply)
		}(i)
	}
	wg.Wait()
}

func main() {
	ch := make(chan string)
	go call(ch)
	start(ch)
}
