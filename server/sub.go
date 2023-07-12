package server

import (
	"example-stomp/server/pool"
	"flag"
	"fmt"
	"log"

	stomp "github.com/go-stomp/stomp/v3"
)

var queueName = flag.String("queue", "/queue/client_test", "Destination queue")

func InitConn() (*pool.TcpConnPool, error) {
	tcpConfig := pool.TcpConfig{
		Host:         "localhost",
		Port:         61613,
		Username:     "",
		Password:     "",
		MaxIdleConns: 10,
		MaxOpenConn:  20,
	}
	connPool, err := pool.CreateTcpConnPool(&tcpConfig)
	if err != nil {
		return nil, err
	}
	return connPool, err
}

func NewConn(connPool *pool.TcpConnPool) *stomp.Conn {
	//1. get connection
	stompClient, err := connPool.Get()
	if err != nil {
		log.Fatal("error get connection ", err)
	}
	log.Println("get stomp connection")
	conn := pool.GetConn(stompClient)
	return conn
}

func ListenKoneksi() {
	//1. init amqp
	connPool, err := InitConn()
	if err != nil {
		log.Fatal("error when init connection ", err)
	}
	for {
		err := Subscribe(connPool)
		if err != nil {
			fmt.Printf("Error: %v\n", err)
		}
		// time.Sleep(5 * time.Second)
	}
}

// Subscribe Message from destination
// func handler handle msg reveived from destination
func Subscribe(connPool *pool.TcpConnPool) error {
	conn := NewConn(connPool)
	sub, err := conn.Subscribe(*queueName, stomp.AckAuto)
	if err != nil {
		fmt.Println(err)
		return err
	}
	defer sub.Unsubscribe()
	for {
		msg := <-sub.C
		if msg.Err != nil {
			fmt.Println(msg)
			return msg.Err
		}
		//begin transaction
		tx := conn.Begin()

		//do another
		HandleMessage(msg.Err, string(msg.Body))

		//acknowledge the message
		err = tx.Ack(msg)
		if err != nil {
			fmt.Println(err)
			return err
		}

		err = tx.Commit()
		if err != nil {
			fmt.Println(err)
			return err
		}
	}
}

// handle message
func HandleMessage(err error, msg string) {
	if err != nil {
		fmt.Println("error message")
		log.Println(err.Error())
	}
	fmt.Println("data was recived...!")
	if msg != "" {
		//print messgae
		fmt.Println(msg)
	}
}
