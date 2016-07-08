package rabbitmqoutput

import (
	"crypto/tls"
	"fmt"
	"time"

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
	ch   *amqp.Channel
}

type options struct {
	// Add a field to an event. Default value is {}
	AddField map[string]interface{} `mapstructure:"add_field"`

	// Extra rabbitmq arguments. Default value is {}
	Arguments amqp.Table `mapstructure:"arguments"`

	// Time in seconds to wait before retrying a connection. Default value is 1
	ConnectRetryInterval int `mapstructure:"connect_retry_interval"`

	// Time in seconds to wait before timing-out. Default value is 0 (no timeout)
	ConnectionTimeout int `mapstructure:"connection_timeout"`

	// Enable or disable logging. Default value is false
	Debug bool `mapstructure:"debug"`

	// Is this exchange durable - should it survive a broker restart? Default value is true
	Durable bool `mapstructure:"durable"`

	// The name of the exchange to send message to. There is no default value for this setting.
	Exchange string `mapstructure:"exchange" validate:"required"`

	// The exchange type (fanout, topic, direct). There is no default value for this setting.
	ExchangeType string `mapstructure:"exchange_type" validate:"required"`

	// Interval (in second) to send heartbeat to rabbitmq. Default value is 0
	// If value if lower than 1, server's interval setting will be used.
	Heartbeat time.Duration `mapstructure:"heartbeat"`

	// RabbitMQ server address. There is no default value for this setting.
	Host string `mapstructure:"host"`

	// The routing key to use when binding a queue to the exchange. Default value is ""
	// This is only relevant for direct or topic exchanges (Routing keys are ignored on fanout exchanges).
	// This setting can be dynamic using the %{foo} syntax.
	Key string `mapstructure:"host"`

	// Use queue passively declared, meaning it must already exist on the server. Default value is false
	// To have Logfan to create the queue if necessary leave this option as false.
	// If actively declaring a queue that already exists, the queue options for this plugin (durable, etc) must match those of the existing queue.
	Passive bool `mapstructure:"passive"`

	// RabbitMQ password. Default value is "guest"
	Password string `mapstructure:"password"`

	// Should RabbitMQ persist messages to disk? Default value is true
	Persistent string `mapstructure:"persistent"`

	// RabbitMQ port to connect on. Default value is 5672
	Port int `mapstructure:"port"`

	// Enable or disable SSL. Default value is false
	SSL bool `mapstructure:"ssl"`

	// Add any number of arbitrary tags to your event. There is no default value for this setting.
	// This can help with processing later. Tags can be dynamic and include parts of the event using the %{field} syntax.
	Tags []string `mapstructure:"tags"`

	// RabbitMQ username. Default value is "guest"
	User string `mapstructure:"user"`

	// Validate SSL certificate. Default value is false
	VerifySSL bool `mapstructure:"verify_ssl"`

	// The vhost to use. Default value is "/"
	Vhost string `mapstructure:"vhost"`
}

func (p *processor) Configure(ctx veino.ProcessorContext, conf map[string]interface{}) error {
	defaults := options{
		ConnectRetryInterval: 1,
		ConnectionTimeout:    0,
		Durable:              true,
		Heartbeat:            0,
		Passive:              false,
		Password:             "guest",
		Port:                 5672,
		SSL:                  false,
		User:                 "guest",
		VerifySSL:            false,
		Vhost:                "/",
	}
	p.opt = &defaults
	return p.ConfigureAndValidate(ctx, conf, p.opt)
}

func (p *processor) Receive(e veino.IPacket) error {
	key := p.opt.Key
	processors.Dynamic(&key, e.Fields())

	body, err := e.Fields().Json()
	if err != nil {
		return err
	}

	err = p.ch.Publish(
		p.opt.Exchange,
		key,
		false, // mandatory
		false, // immediate
		amqp.Publishing{
			Headers:         amqp.Table{},
			ContentType:     "application/json",
			ContentEncoding: "",
			Body:            body,
			DeliveryMode:    amqp.Transient, // 1=non-persistent, 2=persistent
			Priority:        0,              // 0-9
			// a bunch of application/implementation-specific fields
		},
	)

	return err
}

func (p *processor) Start(e veino.IPacket) (err error) {
	p.conn, p.ch, err = p.setup()
	return err
}

func (p *processor) Stop(e veino.IPacket) error {
	p.conn.Close()
	return nil
}

func (p *processor) setup() (*amqp.Connection, *amqp.Channel, error) {
	scheme := map[bool]string{true: "amqps", false: "amqp"}[p.opt.SSL]
	url := fmt.Sprintf("%s://%s:%s@%s:%d/%s", scheme, p.opt.User, p.opt.Password, p.opt.Host, p.opt.Port, p.opt.Vhost)

	if p.opt.Debug {
		p.Logger.Printf("Connecting to %s", url)
	}

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
		if err := ch.ExchangeDeclare(
			p.opt.Exchange,
			p.opt.ExchangeType,
			p.opt.Durable,
			false, // auto-deleted
			false, // internal
			false, // noWait
			p.opt.Arguments,
		); err != nil {
			return nil, nil, err
		}
	}

	return conn, ch, nil
}
