// HTTPPoller allows you to call an HTTP Endpoint, decode the output of it into an event
package httppoller

import (
	"encoding/json"

	"github.com/parnurzeal/gorequest"
	"github.com/veino/processors"
	"github.com/veino/veino"
)

func New() veino.Processor {
	return &processor{opt: &options{}}
}

type options struct {
	Method string
	Url    string
}

type processor struct {
	processors.Base

	logger  veino.Logger
	opt     *options
	request *gorequest.SuperAgent
}

func (p *processor) Configure(ctx map[string]interface{}, conf map[string]interface{}) error {
	return p.Base.ConfigureAndValidate(ctx, conf, p.opt)
}

func (p *processor) Start(e veino.IPacket) error {
	p.request = gorequest.New()
	return nil
}

func (p *processor) Tick(e veino.IPacket) error {
	var (
		errs []error
		resp gorequest.Response
		body string
	)

	switch p.opt.Method {
	case "GET":
		resp, body, errs = p.request.Get(p.opt.Url).End()
	default:
		p.logger.Printf("Method %s not implemented", p.opt.Method)
		return nil
	}

	if errs != nil {
		p.logger.Printf("while http requesting %s : %#v", p.opt.Url, errs)
		return nil
	}
	if resp.StatusCode >= 400 {
		p.logger.Printf("http response code %s : %d (%s)", p.opt.Url, resp.StatusCode, resp.Status)
		return nil
	}

	e.SetMessage(p.opt.Url)
	json.Unmarshal([]byte(body), e.Fields())
	p.Send(e, 0)

	return nil
}
