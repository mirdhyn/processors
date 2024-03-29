package execinput

import (
	"bytes"
	"errors"
	"fmt"
	"os/exec"
	"strings"

	"github.com/veino/processors"
	"github.com/veino/veino"
)

func New() veino.Processor {
	return &processor{opt: &options{}}
}

type options struct {
	Command   string
	Args      []string
	Add_field map[string]interface{}
	Interval  string
	Codec     string
	Tags      []string
	Type      string
}

type processor struct {
	processors.Base

	opt *options
	q   chan bool
}

func (p *processor) Configure(ctx veino.ProcessorContext, conf map[string]interface{}) error {
	return p.ConfigureAndValidate(ctx, conf, p.opt)
}

func (p *processor) Tick(e veino.IPacket) error {
	var (
		err  error
		data string
	)

	data, err = p.doExec()

	if err != nil {
		return fmt.Errorf("Error while executing command '%s' (%s)", p.opt.Command, err.Error())
	}

	e.SetMessage(data)
	e.Fields().SetValueForPath(data, "stdout")
	e.Fields().SetValueForPath(p.opt.Command, "command")
	e.Fields().SetValueForPath(strings.Join(p.opt.Args, ", "), "args")

	processors.ProcessCommonFields(e.Fields(), p.opt.Add_field, p.opt.Tags, p.opt.Type)
	p.Send(e, 0)

	return nil
}

func (p *processor) doExec() (data string, err error) {
	var (
		buferr bytes.Buffer
		raw    []byte
		cmd    *exec.Cmd
	)
	cmd = exec.Command(p.opt.Command, p.opt.Args...)
	cmd.Stderr = &buferr
	if raw, err = cmd.Output(); err != nil {
		return
	}
	data = string(raw)
	if buferr.Len() > 0 {
		err = errors.New(buferr.String())
	}
	return
}
