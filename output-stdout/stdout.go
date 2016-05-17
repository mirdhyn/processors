package stdout

import (
	"fmt"
	"net/http"

	"github.com/k0kubun/pp"
	"github.com/veino/processors"
	"github.com/veino/runtime/memory"
	"github.com/veino/veino"
)

const (
	CODEC_PRETTYPRINT string = "pp"
	CODEC_LINE        string = "line"
	CODEC_RUBYDEBUG   string = "rubydebug"
	CODEC_JSON        string = "json"
)

func New() veino.Processor {
	return &processor{opt: &options{}}
}

type options struct {
	Codec string `validate:"required"`
}

type processor struct {
	processors.Base

	logger veino.Logger
	Memory *memory.Memory
	// WebHook *veino.WebHook
	opt *options
}

func (p *processor) Configure(ctx map[string]interface{}, conf map[string]interface{}) error {
	p.opt.Codec = CODEC_LINE
	if err := p.Base.ConfigureAndValidate(ctx, conf, p.opt); err != nil {
		return err
	}

	if p.opt.Codec == CODEC_RUBYDEBUG {
		p.opt.Codec = CODEC_PRETTYPRINT
	}

	return nil
}

func (p *processor) Receive(e veino.IPacket) error {
	switch p.opt.Codec {
	case CODEC_LINE:
		fmt.Printf("%s %s %s\n",
			e.Fields().ValueOrEmptyForPathString("@timestamp"),
			e.Fields().ValueOrEmptyForPathString("host"),
			e.Message(),
		)
	case CODEC_JSON:
		json, _ := e.Fields().Json()
		fmt.Printf("%s\n", json)
		break
	case CODEC_PRETTYPRINT:
		pp.Printf("%s\n", e.Fields())
		break
	default:
		p.logger.Printf("unknow codec %s", p.opt.Codec)
	}

	p.Memory.Set("", e.Fields().StringIndentNoTypeInfo(2))
	return nil
}

func (p *processor) Start(e veino.IPacket) error {
	// p.WebHook.Add("events", p.HttpHandler)
	return nil
}

func (p *processor) Stop(e veino.IPacket) error { return nil }

// Handle Request received by veino for this agent (url hook should be registered during p.Start)
func (p *processor) HttpHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html")
	o := p.Memory.Items()
	for i, v := range o {
		// log.Printf("debug %s = %s", i, v)
		w.Write([]byte("<h3>" + i + "</h3><pre>" + v.(string) + "</pre>"))
	}
}
