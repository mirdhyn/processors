package fileinput

import (
	"fmt"
	"os"
	"os/user"
	"path/filepath"
	"sync"
	"time"

	"github.com/ShowMax/go-fqdn"

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
	host                string
}

type options struct {
	// Add a field to an event. Default value is {}
	AddField map[string]interface{} `mapstructure:"add_field"`

	// Closes any files that were last read the specified timespan in seconds ago.
	// Default value is 3600 (i.e. 1 hour)
	// This has different implications depending on if a file is being tailed or read.
	// If tailing, and there is a large time gap in incoming data the file can be
	// closed (allowing other files to be opened) but will be queued for reopening
	// when new data is detected. If reading, the file will be closed after
	// close_older seconds from when the last bytes were read.
	CloseOlder int `mapstructure:"close_older"`

	// Not implemented
	Codec string `mapstructure:"codec"`

	// Set the new line delimiter. Default value is "\n"
	Delimiter string `mapstructure:"delimiter"`

	// How often (in seconds) we expand the filename patterns in the path option
	// to discover new files to watch. Default value is 15
	DiscoverInterval int `mapstructure:"discover_interval"`

	// Exclusions (matched against the filename, not full path).
	// Filename patterns are valid here, too.
	Exclude []string `mapstructure:"exclude"`

	// When the file input discovers a file that was last modified before the
	// specified timespan in seconds, the file is ignored.
	// After it’s discovery, if an ignored file is modified it is no longer ignored
	// and any new data is read.
	// Default value is 86400 (i.e. 24 hours)
	IgnoreOlder int `mapstructure:"ignore_older"`

	// What is the maximum number of file_handles that this input consumes at any one time.
	// Use close_older to close some files if you need to process more files than this number.
	MaxOpenFiles string `mapstructure:"max_open_files"`

	// The path(s) to the file(s) to use as an input.
	// You can use filename patterns here, such as /var/log/*.log.
	// If you use a pattern like /var/log/**/*.log, a recursive search of /var/log
	// will be done for all *.log files.
	// Paths must be absolute and cannot be relative.
	// You may also configure multiple paths.
	Path []string `mapstructure:"path" validate:"required"`

	// Path of the sincedb database file
	// The sincedb database keeps track of the current position of monitored
	// log files that will be written to disk.
	SincedbPath string `mapstructure:"sincedb_path"`

	// How often (in seconds) to write a since database with the current position of monitored log files.
	// Default value is 15
	SincedbWriteInterval int `mapstructure:"sincedb_write_interval"`

	// Choose where Logfan starts initially reading files: at the beginning or at the end.
	// The default behavior treats files like live streams and thus starts at the end.
	// If you have old data you want to import, set this to beginning.
	// This option only modifies "first contact" situations where a file is new
	// and not seen before, i.e. files that don’t have a current position recorded in a sincedb file.
	// If a file has already been seen before, this option has no effect and the
	// position recorded in the sincedb file will be used.
	// Default value is "end"
	// Value can be any of: "beginning", "end"
	StartPosition string `mapstructure:"start_position"`

	// How often (in seconds) we stat files to see if they have been modified.
	// Increasing this interval will decrease the number of system calls we make,
	// but increase the time to detect new log lines.
	// Default value is 1
	StatInterval int `mapstructure:"stat_interval"`

	// Add any number of arbitrary tags to your event. There is no default value for this setting.
	// This can help with processing later. Tags can be dynamic and include parts of the event using the %{field} syntax.
	Tags []string `mapstructure:"tags"`

	// Add a type field to all events handled by this input.
	// Types are used mainly for filter activation.
	Type string `mapstructure:"type"`
}

func (p *processor) Configure(ctx veino.ProcessorContext, conf map[string]interface{}) error {
	p.opt.StartPosition = "end"
	p.opt.SincedbPath = ".sincedb.json"
	p.opt.SincedbWriteInterval = 15
	p.opt.StatInterval = 1
	p.host = fqdn.Get()

	if usr, err := user.Current(); err == nil {
		p.opt.SincedbPath = usr.HomeDir + "/" + p.opt.SincedbPath
	}

	return p.ConfigureAndValidate(ctx, conf, p.opt)
}

func (p *processor) Start(e veino.IPacket) error {
	watch.POLL_DURATION = time.Second * time.Duration(p.opt.StatInterval)
	p.q = make(chan bool)

	var matches []string

	for _, currentPath := range p.opt.Path {
		if currentMatches, err := filepath.Glob(currentPath); err == nil {
			matches = append(matches, currentMatches...)
			continue
		}
		return fmt.Errorf("glob(%q) failed", currentPath)
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

	for _, filePath := range matches {
		p.wg.Add(1)
		go p.tailFile(filePath, p.q)
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
		if p.opt.StartPosition == "end" {
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

	for line := range t.Lines {

		e := p.NewPacket(line.Text, map[string]interface{}{
			"host":       p.host,
			"path":       path,
			"@timestamp": line.Time.Format(veino.VeinoTime),
		})

		since.Offset, _ = t.Tell()

		processors.ProcessCommonFields(e.Fields(), p.opt.AddField, p.opt.Tags, p.opt.Type)
		p.Send(e)
		p.checkSaveSinceDBInfos()
	}

	return nil
}
