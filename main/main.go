package main

import (
	"encoding/json"
	"fmt"
	"krpc"
	"krpc/codec"
	"log"
	"net"
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

	//client
	conn, err := net.Dial("tcp", <-addr)
	if err != nil {
		log.Println("net dial err:", err)
		return
	}

	defer func() { _ = conn.Close() }()

	time.Sleep(time.Second)

	_ = json.NewEncoder(conn).Encode(krpc.DefaultOption)
	c := codec.NewJsonCodec(conn)

	for i := 0; i < 5; i++ {
		h := &codec.Header{
			ServiceMethod: "Service.Test",
			Seq:           uint64(i),
		}
		_ = c.Write(h, fmt.Sprintf("krpc req %d", h.Seq))
		_ = c.ReadHeader(h)
		var reply string
		_ = c.ReadBody(&reply)
		log.Println("reply:", reply)
	}
}
