package fileinput

// Code source from github.com/tsaikd/gogstash
// @see https://github.com/tsaikd/gogstash/tree/master/input/file

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"os"
	"sync"
	"time"
)

type sinceDBInfo struct {
	Offset int64 `json:"offset,omitempty"`
}

func (p *processor) loadSinceDBInfos() (err error) {
	var (
		raw []byte
	)
	p.sinceDBInfosMutex = &sync.Mutex{}

	p.sinceDBInfos = map[string]*sinceDBInfo{}

	if p.opt.SincedbPath == "" || p.opt.SincedbPath == "/dev/null" {
		p.Logger.Println("No valid sincedb path")
		return
	}

	if _, err := os.Stat(p.opt.SincedbPath); os.IsNotExist(err) {
		p.Logger.Printf("sincedb not found: %q", p.opt.SincedbPath)
		return err
	}

	if raw, err = ioutil.ReadFile(p.opt.SincedbPath); err != nil {
		p.Logger.Printf("Read sincedb failed: %q\n%s", p.opt.SincedbPath, err)
		return
	}

	if err = json.Unmarshal(raw, &p.sinceDBInfos); err != nil {
		p.Logger.Printf("Unmarshal sincedb failed: %q\n%s", p.opt.SincedbPath, err)
		return
	}

	return
}

func (p *processor) saveSinceDBInfos() (err error) {
	var (
		raw []byte
	)

	p.sinceDBLastSaveTime = time.Now()

	if p.opt.SincedbPath == "" || p.opt.SincedbPath == "/dev/null" {
		p.Logger.Println("No valid sincedb path")
		return
	}

	p.sinceDBInfosMutex.Lock()
	if raw, err = json.Marshal(p.sinceDBInfos); err != nil {
		p.sinceDBInfosMutex.Unlock()
		p.Logger.Printf("Marshal sincedb failed: %s", err)
		return
	}
	p.sinceDBInfosMutex.Unlock()

	p.sinceDBLastInfosRaw = raw

	if err = ioutil.WriteFile(p.opt.SincedbPath, raw, 0664); err != nil {
		p.Logger.Printf("Write sincedb failed: %q\n%s", p.opt.SincedbPath, err)
		return
	}

	return
}

func (p *processor) checkSaveSinceDBInfos() (err error) {
	var (
		raw []byte
	)
	if time.Since(p.sinceDBLastSaveTime) > time.Duration(p.opt.SincedbWriteInterval)*time.Second {
		if raw, err = json.Marshal(p.sinceDBInfos); err != nil {
			p.Logger.Printf("Marshal sincedb failed: %s", err)
			return
		}
		if bytes.Compare(raw, p.sinceDBLastInfosRaw) != 0 {
			err = p.saveSinceDBInfos()
		}
	}
	return
}

func (p *processor) checkSaveSinceDBInfosLoop() (err error) {
	for {
		time.Sleep(time.Duration(p.opt.SincedbWriteInterval) * time.Second)
		if err = p.checkSaveSinceDBInfos(); err != nil {
			return
		}
	}
	return
}
