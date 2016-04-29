package imap_input

import (
	"time"

	"github.com/veino/veino"
	"github.com/vjeantet/postman/watch"
)

func New(l veino.Logger) veino.Processor {
	return &processor{logger: l}
}

type processor struct {
	NewPacket veino.PacketBuilder
	Send      veino.PacketSender
	logger    veino.Logger
	config    *watch.Config
	watcher   *watch.Watch
}

func (p *processor) Configure(conf map[string]interface{}) error {
	p.config = watch.NewConfig()
	p.config.Host = conf["host"].(string)
	p.config.Port = uint(conf["port"].(float64))
	p.config.Ssl = conf["ssl"].(bool)
	p.config.Mailbox = conf["mailbox"].(string)
	p.config.Password = conf["password"].(string)
	p.config.Username = conf["username"].(string)
	p.config.Idletimeout = time.Duration(int(conf["idletimeout"].(float64))) * time.Minute
	return nil
}

func (p *processor) Receive(e veino.IPacket) error { return nil }

func (p *processor) Stop(e veino.IPacket) error {
	p.logger.Printf("imap input - closing connection... please wait (max %d minutes)", p.config.Idletimeout/time.Minute)
	p.watcher.Stop()

	return nil
}

func (p *processor) Tick(e veino.IPacket) error { return nil }

func (p *processor) Start(e veino.IPacket) error {
	p.watcher = watch.New(p.config, newToJsonHandler(p.NewPacket, e, p.Send))
	go p.watcher.Start()
	return nil
}
