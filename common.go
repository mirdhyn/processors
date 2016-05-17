package processors

import (
	"io/ioutil"
	"log"
	"os"

	"github.com/mitchellh/mapstructure"
	"github.com/veino/veino"
	"gopkg.in/go-playground/validator.v8"
)

var (
	// DefaultLogger is used when Config.Logger == nil
	DefaultLogger = log.New(os.Stderr, "", log.LstdFlags)

	// DiscardingLogger can be used to disable logging output
	DiscardingLogger = log.New(ioutil.Discard, "", 0)
)

// Logger represents log.Logger functions from the standard library
type logger interface {
	Fatal(v ...interface{})
	Fatalf(format string, v ...interface{})
	Fatalln(v ...interface{})

	Panic(v ...interface{})
	Panicf(format string, v ...interface{})
	Panicln(v ...interface{})

	Print(v ...interface{})
	Printf(format string, v ...interface{})
	Println(v ...interface{})
}

type Base struct {
	Send      veino.PacketSender
	NewPacket veino.PacketBuilder
	Logger    logger
}

func (b *Base) Configure(ctx map[string]interface{}, conf map[string]interface{}) error {
	return nil
}

func (b *Base) ConfigureAndValidate(ctx map[string]interface{}, conf map[string]interface{}, rawVal interface{}) error {

	// Logger
	if val, ok := ctx["logger"]; ok && val != nil {
		b.Logger = ctx["logger"].(logger)
	} else {
		b.Logger = DefaultLogger
	}

	if err := mapstructure.Decode(conf, rawVal); err != nil {
		return err
	}

	if err := validator.New(&validator.Config{TagName: "validate"}).Struct(rawVal); err != nil {
		return err
	}

	if val, ok := ctx["PacketSender"]; ok && val != nil {
		b.Send = ctx["PacketSender"].(func(veino.IPacket, ...int) bool)
	}

	if val, ok := ctx["PacketBuilder"]; ok && val != nil {
		b.NewPacket = ctx["PacketBuilder"].(func(string, map[string]interface{}) veino.IPacket)
	}

	return nil
}

func (b *Base) Receive(e veino.IPacket) error { return nil }

func (b *Base) Tick(e veino.IPacket) error { return nil }

func (b *Base) Start(e veino.IPacket) error { return nil }

func (b *Base) Stop(e veino.IPacket) error { return nil }

func decodeConf(m interface{}, rawVal interface{}) error {
	return mapstructure.Decode(m, rawVal)
}
