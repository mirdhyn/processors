package imap_input

import (
	"github.com/etrepat/postman/watch"
	"github.com/veino/processors"
	"github.com/veino/veino"
)

func New() veino.Processor {
	return &processor{}
}

type processor struct {
	processors.Base

	config  *watch.Flags
	watcher *watch.Watch
}

func (p *processor) Configure(ctx veino.ProcessorContext, conf map[string]interface{}) error {
	p.config = watch.NewFlags()
	p.config.Host = conf["host"].(string)
	p.config.Port = uint(conf["port"].(float64))
	p.config.Ssl = conf["ssl"].(bool)
	p.config.Mailbox = conf["mailbox"].(string)
	p.config.Password = conf["password"].(string)
	p.config.Username = conf["username"].(string)
	return nil
}

func (p *processor) Stop(e veino.IPacket) error {
	p.Logger.Printf("imap input - closing connection...")
	p.watcher.Stop()

	return nil
}

func (p *processor) Start(e veino.IPacket) error {
	p.watcher = watch.New(p.config, newToJsonHandler(p.NewPacket, e, p.Send))
	go p.watcher.Start()
	return nil
}
