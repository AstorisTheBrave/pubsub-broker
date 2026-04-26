package main

import (
	"fmt"
	"log"
	"net"
	"os"
)

func main() {
	port := "9000"
	if len(os.Args) > 1 {
		port = os.Args[1]
	}

	ln, err := net.Listen("tcp", ":"+port)
	if err != nil {
		log.Fatal(err)
	}

	broker := NewBroker()
	fmt.Printf("pubsub-broker listening on :%s\n", port)

	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Printf("accept: %v", err)
			continue
		}
		go NewClient(conn, broker).Handle()
	}
}
