package main

import (
	"goRedis/core"
	"log"
	"os"
)

func main() {
	// read path from args
	path := os.Args[1]
	config := core.LoadConfig(path)
	server, err := core.NewServer(config)
	if err != nil {
		log.Printf("[MAIN TCP Server ERROR] Init tcp server error, err :%s\n", err)
		return
	}
	server.Loop.AddFileEvent(server.Fd, core.READABLE, core.AcceptHandler, nil)
	// TODO TimeEvents

	// AEMAIN EPOLL主循环
	server.Loop.AeMain()
}
