package pool

import (
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	stomp "github.com/go-stomp/stomp/v3"
)

// tcpConn is a wrapper for a single tcp connection
type stompConn struct {
	id   string       // A unique id to identify a connection
	pool *TcpConnPool // The AMQP connecion pool
	conn stomp.Conn   // The underlying TCP connection
}

// TcpConnPool represents a pool of tcp connections
type TcpConnPool struct {
	host         string
	port         int
	username     string
	password     string
	mu           sync.Mutex            // mutex to prevent race conditions
	idleConns    map[string]*stompConn // holds the idle connections
	numOpen      int                   // counter that tracks open connections
	maxOpenCount int
	maxIdleCount int
	// A queue of connection requests
	requestChan chan *connRequest
}

// put() attempts to return a used connection back to the pool
// It closes the connection if it can't do so
func (p *TcpConnPool) Put(c *stompConn) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.maxIdleCount > 0 && p.maxIdleCount > len(p.idleConns) {
		p.idleConns[c.id] = c // put into the pool
	} else {
		c.conn.Disconnect()
		c.pool.numOpen--
	}
}

// connRequest wraps a channel to receive a connection
// and a channel to receive an error
type connRequest struct {
	connChan chan *stompConn
	errChan  chan error
}

// get() retrieves a TCP connection
func (p *TcpConnPool) Get() (*stompConn, error) {
	p.mu.Lock()

	// Case 1: Gets a free connection from the pool if any
	numIdle := len(p.idleConns)
	if numIdle > 0 {
		// Loop map to get one conn
		for _, c := range p.idleConns {
			// remove from pool
			delete(p.idleConns, c.id)
			p.mu.Unlock()
			return c, nil
		}
	}

	// Case 2: Queue a connection request
	if p.maxOpenCount > 0 && p.numOpen >= p.maxOpenCount {
		// Create the request
		req := &connRequest{
			connChan: make(chan *stompConn, 1),
			errChan:  make(chan error, 1),
		}

		// Queue the request
		p.requestChan <- req

		p.mu.Unlock()

		// Waits for either
		// 1. Request fulfilled, or
		// 2. An error is returned
		select {
		case tcpConn := <-req.connChan:
			return tcpConn, nil
		case err := <-req.errChan:
			return nil, err
		}
	}

	// Case 3: Open a new connection
	p.numOpen++
	p.mu.Unlock()

	newTcpConn, err := p.openNewTcpConnection()
	if err != nil {
		p.mu.Lock()
		p.numOpen--
		p.mu.Unlock()
		return nil, err
	}

	return newTcpConn, nil
}

// openNewTcpConnection() creates a new TCP connection at p.host and p.port
func (p *TcpConnPool) openNewTcpConnection() (*stompConn, error) {
	addr := fmt.Sprintf("%s:%d", p.host, p.port)
	// these are the default options that work with RabbitMQ
	var options []func(*stomp.Conn) error = []func(*stomp.Conn) error{
		stomp.ConnOpt.Login(p.username, p.password),
		stomp.ConnOpt.Host("/"),
	}
	c, err := stomp.Dial("tcp", addr, options...)
	if err != nil {
		return nil, err
	}
	status := fmt.Sprintf("new connection stomp %s", addr)
	fmt.Println(status)

	return &stompConn{
		// Use unix time as id
		id:   fmt.Sprintf("%v", time.Now().UnixNano()),
		conn: *c,
		pool: p,
	}, nil
}

// handleConnectionRequest() listens to the request queue
// and attempts to fulfil any incoming requests
func (p *TcpConnPool) handleConnectionRequest() {
	for req := range p.requestChan {
		var (
			requestDone = false
			hasTimeout  = false

			// start a 3-second timeout
			timeoutChan = time.After(3 * time.Second)
		)

		for {
			if requestDone || hasTimeout {
				break
			}
			select {
			// request timeout
			case <-timeoutChan:
				hasTimeout = true
				req.errChan <- errors.New("connection request timeout")
				log.Println("connection timeout")
			default:
				// 1. get idle conn or open new conn
				// 2. if success, pass conn into req.conn. requestDone!
				// 3. if fail, we retry until timeout
				p.mu.Lock()

				// First, we try to get an idle conn.
				// If fail, we try to open a new conn.
				// If both does not work, we try again in the next loop until timeout.
				numIdle := len(p.idleConns)
				if numIdle > 0 {
					for _, c := range p.idleConns {
						delete(p.idleConns, c.id)
						p.mu.Unlock()
						req.connChan <- c // give conn
						requestDone = true
						break
					}
				} else if p.maxOpenCount > 0 && p.numOpen < p.maxOpenCount {
					p.numOpen++
					p.mu.Unlock()

					c, err := p.openNewTcpConnection()
					if err != nil {
						p.mu.Lock()
						p.numOpen--
						p.mu.Unlock()
					} else {
						req.connChan <- c // give conn
						requestDone = true
					}
				} else {
					p.mu.Unlock()
				}

			}
		}
	}
}

// =================================================================================================
const maxQueueLength = 10_000

// TcpConfig is a set of configuration for a TCP connection pool
type TcpConfig struct {
	Host         string
	Port         int
	Username     string
	Password     string
	MaxIdleConns int
	MaxOpenConn  int
}

// CreateTcpConnPool() creates a connection pool
// and starts the worker that handles connection request
func CreateTcpConnPool(cfg *TcpConfig) (*TcpConnPool, error) {
	pool := &TcpConnPool{
		host:         cfg.Host,
		port:         cfg.Port,
		username:     cfg.Username,
		password:     cfg.Password,
		idleConns:    make(map[string]*stompConn),
		requestChan:  make(chan *connRequest, maxQueueLength),
		maxOpenCount: cfg.MaxOpenConn,
		maxIdleCount: cfg.MaxIdleConns,
	}

	go pool.handleConnectionRequest()

	return pool, nil
}

// get connection amqp from stomp.Conn
func GetConn(stompConn *stompConn) *stomp.Conn {
	return &stompConn.conn
}
