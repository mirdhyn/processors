package amqpinput

import (
	"crypto/tls"
	"fmt"
	"time"

	"github.com/clbanning/mxj"
	"github.com/streadway/amqp"
	"github.com/veino/processors"
	"github.com/veino/veino"
)

func New() veino.Processor {
	return &processor{opt: &options{}}
}

type processor struct {
	processors.Base

	opt  *options
	conn *amqp.Connection
}

type options struct {
	Ack                  bool                   `mapstructure:"ack"`
	AddField             map[string]interface{} `mapstructure:"add_field"` // If this filter is successful, add any arbitrary fields to this event.
	Arguments            amqp.Table             `mapstructure:"arguments"`
	AutoDelete           bool                   `mapstructure:"auto_delete"`
	AutomaticRecovery    bool                   `mapstructure:"automatic_recovery"`
	Codec                string                 `mapstructure:"codec"`
	ConnectRetryInterval int                    `mapstructure:"connect_retry_interval"`
	Durable              bool                   `mapstructure:"durable"`
	Exchange             string                 `mapstructure:"exchange"`
	Exclusive            bool                   `mapstructure:"exclusive"`
	Heartbeat            int                    `mapstructure:"heartbeat"`
	Host                 string                 `mapstructure:"host"`
	Key                  string                 `mapstructure:"key"`
	MetadataEnabled      bool                   `mapstructure:"metadata_enabled"`
	Passive              bool                   `mapstructure:"passive"`
	Password             string                 `mapstructure:"password"`
	Port                 int                    `mapstructure:"port"`
	PrefetchCount        int                    `mapstructure:"prefetch_count"`
	PrefetchSize         int                    `mapstructure:"prefetch_size"`
	Queue                string                 `mapstructure:"queue"`
	SSL                  bool                   `mapstructure:"ssl"`
	Tags                 []string               `mapstructure:"tags"` // If this filter is successful, add arbitrary tags to the event. Tags can be dynamic and include parts of the event using the %{field} syntax.
	User                 string                 `mapstructure:"user"`
	VerifySSL            bool                   `mapstructure:"verify_ssl"`
	Vhost                string                 `mapstructure:"vhost"`
}

func (p *processor) Configure(ctx veino.ProcessorContext, conf map[string]interface{}) error {
	defaults := options{
		Ack:                  true,
		AutoDelete:           false,
		ConnectRetryInterval: 1,
		Codec:                "json",
		Durable:              false,
		Exclusive:            false,
		MetadataEnabled:      false, // Not implemented
		Passive:              false,
		Password:             "guest",
		Port:                 5672,
		PrefetchCount:        256,
		PrefetchSize:         0,
		SSL:                  false,
		User:                 "guest",
		VerifySSL:            false,
		Vhost:                "/",
	}

	p.opt = &defaults
	return p.ConfigureAndValidate(ctx, conf, p.opt)
}

func (p *processor) Start(e veino.IPacket) error {

	go func() {
		for {
			deliveries, err := p.consume()
			if err == nil {
				fmt.Println("Connected")

				for msg := range deliveries {
					event := p.parse(msg.Body)
					processors.AddFields(p.opt.AddField, event.Fields())

					if len(p.opt.Tags) > 0 {
						processors.AddTags(p.opt.Tags, event.Fields())
					}

					if p.Send(event, 0) {
						if p.opt.Ack {
							msg.Ack(false)
						}
					}
				}
			} else {
				fmt.Println(err)
			}
			time.Sleep(time.Duration(p.opt.ConnectRetryInterval) * time.Second)
		}
	}()

	return nil
}

func (p *processor) setup() (*amqp.Connection, *amqp.Channel, error) {
	scheme := map[bool]string{true: "amqps", false: "amqp"}[p.opt.SSL]
	url := fmt.Sprintf("%s://%s:%s@%s:%d/%s", scheme, p.opt.User, p.opt.Password, p.opt.Host, p.opt.Port, p.opt.Vhost)

	fmt.Println("Connecting to " + url)

	amqpConfig := amqp.Config{Heartbeat: time.Duration(p.opt.Heartbeat) * time.Second}
	if p.opt.SSL {
		amqpConfig.TLSClientConfig = &tls.Config{InsecureSkipVerify: !p.opt.VerifySSL}
	}

	conn, err := amqp.DialConfig(url, amqpConfig)
	if err != nil {
		return nil, nil, err
	}

	ch, err := conn.Channel()
	if err != nil {
		return nil, nil, err
	}

	if !p.opt.Passive {
		_, err = ch.QueueDeclare(
			p.opt.Queue,
			p.opt.Durable,
			p.opt.AutoDelete,
			p.opt.Exclusive,
			false, // no-wait
			p.opt.Arguments,
		)
		if err != nil {
			return nil, nil, err
		}

		err = ch.QueueBind(
			p.opt.Queue,
			p.opt.Key,
			p.opt.Exchange,
			false,
			nil,
		)
		if err != nil {
			return nil, nil, err
		}
	}

	return conn, ch, nil
}

func (p *processor) consume() (<-chan amqp.Delivery, error) {
	conn, ch, err := p.setup()
	if err != nil {
		return nil, err
	}
	p.conn = conn

	if err := ch.Qos(p.opt.PrefetchCount, p.opt.PrefetchSize, true); err != nil {
		return nil, err
	}

	deliveries, err := ch.Consume(
		p.opt.Queue,
		"", // consumer
		!p.opt.Ack,
		p.opt.Exclusive,
		false, // no-local
		false, // no-wait
		p.opt.Arguments,
	)

	return deliveries, err
}

func (p *processor) parse(message []byte) veino.IPacket {
	var event veino.IPacket

	switch p.opt.Codec {
	case "json":
		fields, err := mxj.NewMapJson(message)
		if err != nil {
			event = p.NewPacket(string(message), nil)
		} else {
			event = p.NewPacket(string(message), fields)
		}

	default:
		event = p.NewPacket(string(message), nil)
	}

	return event
}

func (p *processor) Stop(e veino.IPacket) error {
	p.conn.Close()
	return nil
}
