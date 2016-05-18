package fileinput

import (
	"fmt"
	"os"
	"os/user"
	"path/filepath"
	"sync"
	"time"

	"github.com/veino/processors"
	"github.com/veino/veino"

	"github.com/hpcloud/tail"
	"github.com/hpcloud/tail/watch"
)

func New() veino.Processor {
	return &processor{opt: &options{}}
}

type processor struct {
	processors.Base

	opt                 *options
	sinceDBInfos        map[string]*sinceDBInfo
	sinceDBLastInfosRaw []byte
	sinceDBLastSaveTime time.Time
	q                   chan bool
	wg                  sync.WaitGroup
	sinceDBInfosMutex   *sync.Mutex
}

type options struct {
	Add_field              map[string]interface{}
	Close_older            int // 3600
	Codec                  string
	Delimiter              string // \n
	Discover_interval      int    // 15
	Exclude                []string
	Ignore_older           int // 86400
	Max_open_files         string
	Path                   []string `validate:"required"`
	Sincedb_path           string
	Sincedb_write_interval int    // 15
	Start_position         string // end
	Stat_interval          int    // 1
	Tags                   []string
	Type                   string
}

func (p *processor) Configure(ctx map[string]interface{}, conf map[string]interface{}) error {
	p.opt.Start_position = "end"
	p.opt.Sincedb_path = ".sincedb.json"
	p.opt.Sincedb_write_interval = 15
	p.opt.Stat_interval = 1

	if usr, err := user.Current(); err == nil {
		p.opt.Sincedb_path = usr.HomeDir + "/" + p.opt.Sincedb_path
	}

	return p.ConfigureAndValidate(ctx, conf, p.opt)
}
func (p *processor) Start(e veino.IPacket) error {
	watch.POLL_DURATION = time.Second * time.Duration(p.opt.Stat_interval)
	p.q = make(chan bool)

	var matches []string

	for _, current_path := range p.opt.Path {
		if currentMatches, err := filepath.Glob(current_path); err == nil {
			matches = append(matches, currentMatches...)
			continue
		}
		return fmt.Errorf("glob(%q) failed", current_path)
	}

	if len(p.opt.Exclude) > 0 {
		for i, name := range matches {
			for _, pattern := range p.opt.Exclude {
				if match, _ := filepath.Match(pattern, name); match == true {
					matches = append(matches[:i], matches[i+1:]...)
				}
			}
		}
	}

	p.loadSinceDBInfos()

	for _, file_path := range matches {
		p.wg.Add(1)
		go p.tailFile(file_path, p.q)
	}

	go p.checkSaveSinceDBInfosLoop()

	return nil
}

func (p *processor) Stop(e veino.IPacket) error {
	close(p.q)
	p.wg.Wait()
	p.saveSinceDBInfos()
	return nil
}

// func (p *processor) Tick(e veino.IPacket) error    { return nil }
// func (p *processor) Receive(e veino.IPacket) error { return nil }

func (p *processor) tailFile(path string, q chan bool) error {
	defer p.wg.Done()
	var (
		since  *sinceDBInfo
		ok     bool
		whence int
	)

	p.sinceDBInfosMutex.Lock()
	if since, ok = p.sinceDBInfos[path]; !ok {
		p.sinceDBInfos[path] = &sinceDBInfo{}
		since = p.sinceDBInfos[path]
	}
	p.sinceDBInfosMutex.Unlock()

	if since.Offset == 0 {
		if p.opt.Start_position == "end" {
			whence = os.SEEK_END
		} else {
			whence = os.SEEK_SET
		}
	} else {
		whence = os.SEEK_SET
	}

	t, err := tail.TailFile(path, tail.Config{
		Logger: p.Logger,
		Location: &tail.SeekInfo{
			Offset: since.Offset,
			Whence: whence,
		},
		Follow: true,
		ReOpen: true,
		Poll:   true,
	})
	if err != nil {
		return err
	}

	go func() {
		<-q
		t.Stop()
	}()

	host, err := os.Hostname()
	if err != nil {
		p.Logger.Printf("can not get hostname : %s", err.Error())
	}

	for line := range t.Lines {

		e := p.NewPacket(line.Text, map[string]interface{}{
			"host":       host,
			"path":       path,
			"@timestamp": line.Time.Format(veino.VeinoTime),
		})

		since.Offset, _ = t.Tell()

		processors.ProcessCommonFields(e.Fields(), p.opt.Add_field, p.opt.Tags, p.opt.Type)
		p.Send(e)
		p.checkSaveSinceDBInfos()
	}

	return nil
}
