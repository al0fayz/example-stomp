package server

import (
	"fmt"
	"log"
)

func SendMessage() {
	//1. init amqp
	connPool, err := InitConn()
	if err != nil {
		log.Fatal("error when init connection ", err)
	}
	conn := NewConn(connPool)
	err = conn.Send(DESTINATION, "text/plain", []byte("hello world"), nil)
	if err != nil {
		println("failed to send to server", err)
		return
	}
	fmt.Println("send message")
}
