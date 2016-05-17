// Drops everything received
package drop

import (
	"math/rand"

	"github.com/veino/field"
	"github.com/veino/processors"
	"github.com/veino/veino"
)

func New() veino.Processor {
	return &processor{opt: &options{}}
}

type processor struct {
	processors.Base

	opt *options
}

type options struct {
	// If this event survice to drop, add any arbitrary fields to this event.
	// Field names can be dynamic and include parts of the event using the %{field}.
	Add_field map[string]interface{}

	// If this event survice to drop, add arbitrary tags to the event.
	// Tags can be dynamic and include parts of the event using the %{field} syntax.
	Add_tag []string

	// If this event survice to drop, remove arbitrary fields from this event.
	Remove_field []string

	// If this event survice to drop, remove arbitrary tags from the event.
	// Tags can be dynamic and include parts of the event using the %{field} syntax
	Remove_Tag []string

	// Drop all the events within a pre-configured percentage.
	// This is useful if you just need a percentage but not the whole.
	Percentage int
}

func (p *processor) Configure(ctx map[string]interface{}, conf map[string]interface{}) error {
	p.opt.Percentage = 100
	return p.Base.ConfigureAndValidate(ctx, conf, p.opt)
}

func (p *processor) Receive(e veino.IPacket) error {

	if p.opt.Percentage == 100 || rand.Intn(100) < p.opt.Percentage {
		return nil
	}

	field.ProcessCommonFields2(e.Fields(),
		p.opt.Add_field,
		p.opt.Add_tag,
		p.opt.Remove_field,
		p.opt.Remove_Tag,
	)
	p.Send(e, 0)
	return nil
}
