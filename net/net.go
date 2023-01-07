package net

import (
	"log"

	"golang.org/x/sys/unix"
)

// net libary based on C Unix net programming

func Read(fd int, buf []byte) (int, error) {
	return unix.Read(fd, buf)
}

func Write(fd int, buf []byte) (int, error) {
	return unix.Write(fd, buf)
}

// TcpServer 初始化TcpServer 返回监听socket的文件描述符serverSocket
// Unix C 网络编程
func TcpServer(port int) int {
	// 监听socket文件描述符初始化
	serverSocket, err := unix.Socket(unix.AF_INET, unix.SOCK_STREAM, 0)
	if err != nil {
		log.Printf("[INIT TCPSERVER ERROR] Init tcp server listening socket error, err = %s", err)
		return -1
	}
	err = unix.SetsockoptInt(serverSocket, unix.SOL_SOCKET, unix.SO_REUSEPORT, port)
	if err != nil {
		log.Printf("[INIT TCPSERVER ERROR] Init tcp server listening socket error, err = %s", err)
		unix.Close(serverSocket)
		return -1
	}
	var address unix.SockaddrInet4
	address.Port = port
	err = unix.Bind(serverSocket, &address)
	if err != nil {
		log.Printf("[INIT TCPSERVER ERROR] Tcp server listening socket bind address error, err = %s", err)
		unix.Close(serverSocket)
		return -1
	}
	err = unix.Listen(serverSocket, 64)
	if err != nil {
		log.Printf("[INIT TCPSERVER ERROR] Tcp server listening socket listen error, err = %s", err)
		unix.Close(serverSocket)
		return -1
	}
	return serverSocket
}

/* test code */
// Connect host connect server
func Connect(host [4]byte, port int) int {
	// 初始化socket
	socket, err := unix.Socket(unix.AF_INET, unix.SOCK_STREAM, 0)
	if err != nil {
		log.Printf("[TCPSERVER CONNECTION ERROR] Tcp Server connect init socket error, err = %s", err)
		return -1
	}
	var address unix.SockaddrInet4
	address.Addr = host
	address.Port = port
	err = unix.Connect(socket, &address)
	if err != nil {
		log.Printf("[TCPSERVER CONNECTION ERROR] Tcp Server connect error, err = %s", err)
		unix.Close(socket)
		return -1
	}
	return socket
}

// Accept 接受客户端连接, 返回连接文件描述符nfd(socket)
// fd listening fd
func Accept(fd int) (int, error) {
	nfd, _, err := unix.Accept(fd)
	return nfd, err
}

// Close 断开连接
func Close(fd int) error {
	return unix.Close(fd)
}
