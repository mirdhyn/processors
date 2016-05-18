package processors

import (
	"github.com/mitchellh/mapstructure"
	"github.com/veino/veino"
	"gopkg.in/go-playground/validator.v8"
)

type Base struct {
	Send      veino.PacketSender
	NewPacket veino.PacketBuilder
	Logger    logger
}

func (b *Base) ConfigureAndValidate(ctx map[string]interface{}, conf map[string]interface{}, rawVal interface{}) error {

	// Logger
	if val, ok := ctx["logger"]; ok && val != nil {
		b.Logger = ctx["logger"].(logger)
	} else {
		b.Logger = DefaultLogger
	}

	// Packet Sender func
	if val, ok := ctx["PacketSender"]; ok && val != nil {
		b.Send = ctx["PacketSender"].(func(veino.IPacket, ...int) bool)
	} else {
		// TODO set a dummy packetSender
	}

	// Packet Builder func
	if val, ok := ctx["PacketBuilder"]; ok && val != nil {
		b.NewPacket = ctx["PacketBuilder"].(func(string, map[string]interface{}) veino.IPacket)
	} else {
		// TODO set a dummy PacketBuilder
	}

	// Set processor's user options
	if err := mapstructure.Decode(conf, rawVal); err != nil {
		return err
	}

	// validates processor's user options
	if err := validator.New(&validator.Config{TagName: "validate"}).Struct(rawVal); err != nil {
		return err
	}

	return nil
}

func (b *Base) Configure(ctx map[string]interface{}, conf map[string]interface{}) error { return nil }

func (b *Base) Receive(e veino.IPacket) error { return nil }

func (b *Base) Tick(e veino.IPacket) error { return nil }

func (b *Base) Start(e veino.IPacket) error { return nil }

func (b *Base) Stop(e veino.IPacket) error { return nil }
