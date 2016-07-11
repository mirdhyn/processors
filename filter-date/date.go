package date

import (
	"time"

	"github.com/veino/processors"
	"github.com/veino/veino"
)

func New() veino.Processor {
	return &processor{opt: &options{}}
}

type processor struct {
	processors.Base

	matchFieldName string
	matchPatterns  []string
	opt            *options
}

type options struct {
	// If this filter is successful, add any arbitrary fields to this event.
	AddField map[string]interface{} `mapstructure:"add_field"`

	// The date formats allowed are anything allowed by Golang time format.
	// You can see the docs for this format https://golang.org/src/time/format.go#L20
	// An array with field name first, and format patterns following, [ field, formats... ]
	Match []string `mapstructure:"match"`

	// If this filter is successful, add arbitrary tags to the event. Tags can be dynamic
	// and include parts of the event using the %{field} syntax.
	AddTag []string `mapstructure:"add_tag"`

	// If this filter is successful, remove arbitrary fields from this event.
	RemoveField []string `mapstructure:"remove_field"`

	RemoveTag []string `mapstructure:"remove_tag"`

	// Append values to the tags field when there has been no successful match
	// Default value is ["_dateparsefailure"]
	TagOnFailure []string `mapstructure:"tag_on_failure"`

	// Store the matching timestamp into the given target field. If not provided,
	// default to updating the @timestamp field of the event
	Target string `mapstructure:"target"`

	// Specify a time zone canonical ID to be used for date parsing.
	// The valid IDs are listed on IANA Time Zone database, such as "America/New_York".
	// This is useful in case the time zone cannot be extracted from the value,
	// and is not the platform default. If this is not specified the platform default
	//  will be used. Canonical ID is good as it takes care of daylight saving time
	// for you For example, America/Los_Angeles or Europe/Paris are valid IDs.
	// This field can be dynamic and include parts of the event using the %{field} syntax
	Timezone string `mapstructure:"timezone"`
}

func (p *processor) Configure(ctx veino.ProcessorContext, conf map[string]interface{}) error {
	p.opt.Target = "@timestamp"
	p.opt.TagOnFailure = []string{"_dateparsefailure"}

	if err := p.ConfigureAndValidate(ctx, conf, p.opt); err != nil {
		return err
	}

	p.matchFieldName = p.opt.Match[0]
	p.matchPatterns = p.opt.Match[1:]

	return nil
}

func (p *processor) Receive(e veino.IPacket) error {
	dated := false
	var value string
	var err error
	value, err = e.Fields().ValueForPathString(p.matchFieldName)
	if err == nil {
		for _, layout := range p.matchPatterns {
			var t time.Time

			if p.opt.Timezone != "" {
				location, err := time.LoadLocation(p.opt.Timezone)
				if err == nil {
					t, err = time.ParseInLocation(layout, value, location)
				}
			} else {
				t, err = time.Parse(layout, value)
			}

			if err != nil {
				continue
			}

			dated = true
			e.Fields().SetValueForPath(t.Format(veino.VeinoTime), p.opt.Target)
			processors.ProcessCommonFields2(e.Fields(),
				p.opt.AddField,
				p.opt.AddTag,
				p.opt.RemoveField,
				p.opt.RemoveTag,
			)
			break
		}
	}

	if dated == false {
		processors.AddTags(p.opt.TagOnFailure, e.Fields())
	}

	p.Send(e, 0)
	return nil
}
