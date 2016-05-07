package beatsinput

import (
	"github.com/mitchellh/mapstructure"
	"github.com/veino/veino"
)

func New(l veino.Logger) veino.Processor {
	return &processor{Logger: l}
}

type processor struct {
	Logger    veino.Logger
	Send      veino.PacketSender
	NewPacket veino.PacketBuilder

	opt *options
	q   chan bool
}

type options struct {
	Add_field map[string]interface{}
	Codec     string
	Tags      []string
	Type      string
	Port      int
	Host      string
	SSLCrt    string
	SSLKey    string
}

func (p *processor) Configure(conf map[string]interface{}) error {
	cf := options{
		Port: 5044,
		Host: "127.0.0.1",
	}

	if mapstructure.Decode(conf, &cf) != nil {
		return nil
	}
	p.opt = &cf

	return nil
}

func (p *processor) Start(e veino.IPacket) error {
	p.q = make(chan bool)
	go p.serve()
	return nil
}

func (p *processor) Stop(e veino.IPacket) error {
	p.q <- true
	<-p.q
	return nil
}

func (p *processor) Tick(e veino.IPacket) error    { return nil }
func (p *processor) Receive(e veino.IPacket) error { return nil }
