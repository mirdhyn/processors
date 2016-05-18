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

func (b *Base) Configure(ctx veino.ProcessorContext, conf map[string]interface{}) error { return nil }

func (b *Base) Receive(e veino.IPacket) error { return nil }

func (b *Base) Tick(e veino.IPacket) error { return nil }

func (b *Base) Start(e veino.IPacket) error { return nil }

func (b *Base) Stop(e veino.IPacket) error { return nil }

func (b *Base) ConfigureAndValidate(ctx veino.ProcessorContext, conf map[string]interface{}, rawVal interface{}) error {

	// Logger
	if ctx.Logger != nil {
		b.Logger = ctx.Logger()
	} else {
		b.Logger = DefaultLogger
	}

	// Packet Sender func
	if ctx.PacketSender != nil {
		b.Send = ctx.PacketSender()
	} else {
		// TODO set a dummy packetSender
	}

	// Packet Builder func
	if ctx.PacketBuilder != nil {
		b.NewPacket = ctx.PacketBuilder()
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
