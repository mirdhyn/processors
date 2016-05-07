package beatsinput

import (
	"crypto/tls"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/veino/field"
)

func (p *processor) serve() error {
	var ln net.Listener

	lnUnsecure, err := net.Listen("tcp", fmt.Sprintf("%s:%d", p.opt.Host, p.opt.Port))
	if err != nil {
		return fmt.Errorf("Listener failed: %v", err)
	}

	if p.opt.SSLCrt != "" {
		cert, err := tls.LoadX509KeyPair(p.opt.SSLCrt, p.opt.SSLKey)
		if err != nil {
			return fmt.Errorf("Error loading keys: %v", err)
		}
		config := tls.Config{Certificates: []tls.Certificate{cert}}
		ln = tls.NewListener(lnUnsecure, &config)
	} else {
		ln = lnUnsecure
	}

	clientTerm := make(chan bool)
	var wg sync.WaitGroup
	for {
		select {
		case <-p.q:
			ln.Close()
			close(clientTerm)
			wg.Wait()
			close(p.q)
			return nil
		default:
		}

		if l, ok := ln.(*net.TCPListener); ok {
			l.SetDeadline(time.Now().Add(1 * time.Second))
		}

		conn, err := ln.Accept()
		if err != nil {
			if opErr, ok := err.(*net.OpError); ok && opErr.Timeout() {
				continue
			}
			// log.Printf("Error accepting connection: %v", err)
			continue
		}
		wg.Add(1)
		go p.clientServe(conn, &wg, clientTerm)
	}

	return nil
}

// lumberConn handles an incoming connection from a lumberjack client
func (p *processor) clientServe(c net.Conn, wg *sync.WaitGroup, clientTerm chan bool) {
	defer wg.Done()
	defer c.Close()

	// log.Printf("[%s] accepting lumberjack connection", c.RemoteAddr().String())

	dataChan := make(chan map[string]interface{}, 3)
	go NewParser(c, dataChan).Parse()

	for {
		select {
		case fields := <-dataChan:
			if fields == nil {
				// log.Printf("[%s] closing lumberjack connection", c.RemoteAddr().String())
				return
			}
			e := p.NewPacket("", fields)
			field.ProcessCommonFields(e.Fields(), p.opt.Add_field, p.opt.Tags, p.opt.Type)
			p.Send(e)
		case <-clientTerm:
			c.SetReadDeadline(time.Now())
		}
	}

}
