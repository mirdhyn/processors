package mongodb

// import "gopkg.in/mgo.v2"

// https://www.elastic.co/guide/en/logstash/current/plugins-outputs-mongodb.html

import (
	"github.com/veino/processors"
	"github.com/veino/veino"
	"gopkg.in/mgo.v2"
)

func New() veino.Processor {
	return &processor{opt: &options{}}
}

type processor struct {
	processors.Base

	session    *mgo.Session
	collection *mgo.Collection
	opt        *options
}

type options struct {
	// The codec used for output data. Output codecs are a convenient method
	// for encoding your data before it leaves the output, without needing a
	// separate filter in your veino pipeline
	Codec string

	// The collection to use. This value can use %{foo} values to dynamically
	// select a collection based on data in the event
	Collection string

	// The database to use
	Database string

	// If true, an "_id" field will be added to the document before insertion.
	// The "_id" field will use the timestamp of the event and overwrite an
	// existing "_id" field in the event
	GenerateId bool // false

	// If true, store the @timestamp field in mongodb as an ISODate type
	// instead of an ISO8601 string. For more information about this,
	// see http://www.mongodb.org/display/DOCS/Dates
	Isodate bool // false

	// Number of seconds to wait after failure before retrying
	Retry_delay int // 3

	// a MongoDB URI to connect to See http://docs.mongodb.org/manual/reference/connection-string/
	Uri string
}

func (p *processor) Configure(ctx map[string]interface{}, conf map[string]interface{}) error {
	p.opt.Retry_delay = 3
	p.opt.Isodate = false
	p.opt.GenerateId = false
	return p.Base.ConfigureAndValidate(ctx, conf, p.opt)
}

func (p *processor) Start(e veino.IPacket) error {
	var err error
	p.session, err = mgo.Dial(p.opt.Uri)
	if err != nil {
		return err
	}

	// Optional. Switch the session to a monotonic behavior.
	p.session.SetMode(mgo.Monotonic, true)
	p.collection = p.session.DB(p.opt.Database).C(p.opt.Collection)

	return nil
}

func (p *processor) Receive(e veino.IPacket) error {
	err := p.collection.Insert(e.Fields())

	return err
}

func (p *processor) Stop(e veino.IPacket) error {
	p.session.Close()
	return nil
}
