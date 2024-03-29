package uuid

import (
	"github.com/nu7hatch/gouuid"
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
	// If this filter is successful, add any arbitrary fields to this event.
	// Field names can be dynamic and include parts of the event using the %{field}.
	Add_field map[string]interface{}

	// If this filter is successful, add arbitrary tags to the event.
	// Tags can be dynamic and include parts of the event using the %{field} syntax.
	Add_tag []string

	// If this filter is successful, remove arbitrary fields from this event.
	Remove_field []string

	// If this filter is successful, remove arbitrary tags from the event.
	// Tags can be dynamic and include parts of the event using the %{field} syntax
	Remove_Tag []string

	// If the value in the field currently (if any) should be overridden by the generated UUID.
	// Defaults to false (i.e. if the field is present, with ANY value, it won’t be overridden)
	Overwrite bool

	// Add a UUID to a field
	Target string
}

func (p *processor) Configure(ctx veino.ProcessorContext, conf map[string]interface{}) error {
	p.opt.Overwrite = false
	return p.ConfigureAndValidate(ctx, conf, p.opt)
}

func (p *processor) Receive(e veino.IPacket) error {
	id, err := uuid.NewV4()

	if err == nil {
		if !(p.opt.Overwrite == false && e.Fields().Exists(p.opt.Target) == true) {
			e.Fields().SetValueForPath(id.String(), p.opt.Target)
		}

		processors.ProcessCommonFields2(e.Fields(),
			p.opt.Add_field,
			p.opt.Add_tag,
			p.opt.Remove_field,
			p.opt.Remove_Tag,
		)
	}

	p.Send(e, 0)
	return nil
}
