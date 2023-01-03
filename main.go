package main

import (
	"goRedis/core"
	"log"
	"os"
)

func main() {
	// read path from args
	var path string
	if len(os.Args) > 1 {
		path = os.Args[1]
	}
	config := core.LoadConfig(path)
	log.Printf("[MAIN LOAD CONFIG] Load config success, port = %d, maxConnection = %d\n", config.Port, config.MaxConnection)
	server, err := core.NewServer(config)
	if err != nil {
		log.Printf("[MAIN INIT TCP SERVER ERROR] Init tcp server error, err :%s\n", err)
		return
	}
	server.Loop.AddFileEvent(server.Fd, core.READABLE, core.AcceptHandler, nil)
	log.Printf("[MAIN INIT TCP SERVER] Init tcp server success\n")
	// TODO TimeEvents
	// AEMAIN EPOLL主循环
	server.Loop.AeMain()
}
