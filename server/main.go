package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

var (
	logger    log.Logger
	sigCh     chan os.Signal
	limitChan chan bool
	wait      sync.WaitGroup
)

func main() {
	var port = flag.Int("port", 11211, "TCP port to listen(11121 by default)")
	var logfile = flag.String("log", "", "File to save log(stderr by default)")
	var maxconns = flag.Int("maxconns", 1024, "Max simultaneous connections number(1024 by default)")
	flag.Parse()

	if len(*logfile) != 0 {
		file, err := os.OpenFile(*logfile, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0666)
		if err != nil {
			log.Fatalf("Error appear when opening log file %s", *logfile)
		}
		logger := log.New(file, "gomemcached: ", log.LstdFlags)
	} else {
		logger := log.New(os.Stderr, "gomemcached: ", log.LstdFlags)
	}

	setExitSignal()

	// Used to limit the max simultaneous connections number
	limitChan = make(chan bool, *maxconns)
	for i := 0; i < *maxconns; i++ {
		limitChan <- true
	}

	ln, err := net.ListenTCP("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		logger.Panic(err)
	}
	logger.Printf("Listening on port %d\n", *port)

	// Check whether to quit every 100ms
	ln.SetDeadline(time.Now() + time.Millisecond*100)

	for {
		select {
		case <-sigCh:
			logger.Println("Received the exit signal")
			ln.Close()
			wait.Wait()
			close(limitChan)
			break
		default:
			conn, err := ln.Accept()
			if err != nil {
				logger.Printf("Accept error: %v\n", err)
				continue
			}
			go serve(conn)
		}
	}
	logger.Printf("server exit properly")
}

func setExitSignal() {
	sigChan = make(chan os.Signal)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
}

func serve(conn net.Conn) {
	wait.Add(1)
	<-limitChan
	handleConn(conn)
	limitChan <- true
	wait.Done()
}

func handleConn(conn net.Conn) {
	logger.Printf("Handling connection from %v\n", conn.RemoteAddr())
	defer func() {
		if err := recover(); err != nil {
			logger.Printf("Work error: %v\n", err)
		}
		conn.Close()
		logger.Printf("Connection from %v closed\n", conn.RemoteAddr())
	}()
	conn.SetReadDeadline(time.Now().Add(time.Millisecond * 100))
	request := make([]byte, 2048*1024)
	nbytes, err := conn.Read(request)
	if err != nil {
		log.Printf("conn from %v read %d bytes,  error: %s", conn.RemoteAddr(), nbytes, err)
		return
	}
	cmd, err := decode(request)
	if err != nil {
		log.Printf("decode error: %s", err)
		return
	}
	runCommand(conn, cmd)
}
