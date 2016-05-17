package fileoutput

import (
	"os"
	"strings"

	"github.com/veino/processors"
	"github.com/veino/veino"
)

var lines = map[string][]string{}

func New() veino.Processor {
	return &processor{}
}

type processor struct {
	processors.Base

	Path           string
	Flush_interval interface{} // maybe a cron style or a number
}

func (p *processor) Configure(ctx map[string]interface{}, conf map[string]interface{}) error {
	return p.Base.ConfigureAndValidate(ctx, conf, p)
}

func (p *processor) Receive(e veino.IPacket) error {
	// When agent is Interval, only memorize e
	if p.Flush_interval != nil {
		lines["global"] = append(lines["global"], e.Message())
		return nil
	}

	writeToFile(p.Path, e.Message())
	return nil
}

func (p *processor) Tick(e veino.IPacket) error {
	if len(lines["global"]) == 0 {
		return nil
	}

	content := strings.Join(lines["global"], "\n")
	writeToFile(p.Path, content)
	lines["global"] = []string{}
	return nil
}

func writeToFile(path string, content string) {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0600)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	if _, err = f.WriteString(content + "\n"); err != nil {
		panic(err)
	}
}
