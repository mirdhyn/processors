package geoip

import (
	"errors"
	"fmt"
	"net"
	"strings"

	"github.com/hraban/lrucache"
	"github.com/oschwald/geoip2-golang"
	"github.com/veino/processors"
	"github.com/veino/veino"
)

// New returns the processor struct
func New() veino.Processor {
	return &processor{opt: &options{}}
}

type processor struct {
	processors.Base

	opt       *options
	cache     *lrucache.Cache
	databases map[string]*geoip2.Reader
}

type options struct {
	// If this filter is successful, add any arbitrary fields to this event.
	// Field names can be dynamic and include parts of the event using the %{field}.
	AddField map[string]interface{} `mapstructure:"add_field"`

	// If this filter is successful, add arbitrary tags to the event.
	// Tags can be dynamic and include parts of the event using the %{field} syntax.
	AddTag []string `mapstructure:"add_tag"`

	// If this filter is successful, remove arbitrary fields from this event.
	RemoveField []string `mapstructure:"remove_field"`

	// If this filter is successful, remove arbitrary tags from the event.
	// Tags can be dynamic and include parts of the event using the %{field} syntax
	RemoveTag []string `mapstructure:"remove_tag"`

	// Map of paths to the GeoIP database files, keyed by geoip_type.
	// Country, City, ASN, ISP and organization databases are supported.
	Databases map[string]string `mapstructure:"databases"`

	// The field containing the IP address or hostname to map via geoip.
	Source string `mapstructure:"source"`

	// An array of geoip fields to be included in the event.
	// Possible fields depend on the database type. By default, all geoip fields are included in the event.
	Fields []string `mapstructure:"fields"`

	// Define the target field for placing the parsed data. If this setting is omitted,
	// the geoip data will be stored at the root (top level) of the event
	Target string `mapstructure:"target"`

	// Language to use for city/region/continent names
	Language string `mapstructure:"language"`

	// Cache size
	CacheSize int64 `mapstructure:"cache_size"`
}

type geoipRecords struct {
	city *geoip2.City
	isp  *geoip2.ISP
}

func (p *processor) Configure(ctx veino.ProcessorContext, conf map[string]interface{}) error {
	defaults := options{
		Fields: []string{
			"city_name",
			"country_code",
			"country_name",
			"continent_code",
			"continent_name",
			"latitude",
			"longitude",
			"timezone",
			"postal_code",
			"region_code",
			"region_name",
			"is_anonymous_proxy",
			"is_satellite_provider",
			"asn",
			"organization",
			"isp",
		},
		Language: "en",
	}
	p.opt = &defaults

	p.cache = lrucache.New(p.opt.CacheSize)
	p.databases = map[string]*geoip2.Reader{}

	if err := p.load(conf["databases"]); err != nil {
		processors.DefaultLogger.Fatalln(err)
	}
	return p.ConfigureAndValidate(ctx, conf, p.opt)
}

func (p *processor) Receive(e veino.IPacket) error {
	ip, err := e.Fields().ValueForPathString(p.opt.Source)
	fmt.Println(ip)
	if err != nil {
		return err
	}

	p.cache.OnMiss(p.getInfo())

	records, err := p.cache.Get(ip)
	if err != nil {
		return err
	}

	geoip := records.(geoipRecords)
	fmt.Println(geoip)

	data := make(map[string]interface{})
	lang := p.opt.Language

	fmt.Println(p.opt.Fields)
	for _, field := range p.opt.Fields {
		switch field {
		// City database
		case "city_name":
			if geoip.city != nil {
				data["city_name"] = geoip.city.City.Names[lang]
			}
		case "country_code":
			if geoip.city != nil {
				data["country_code"] = geoip.city.Country.IsoCode
			}
		case "country_name":
			if geoip.city != nil {
				data["country_name"] = geoip.city.Country.Names[lang]
			}
		case "continent_code":
			if geoip.city != nil {
				data["continent_code"] = geoip.city.Continent.Code
			}
		case "continent_name":
			if geoip.city != nil {
				data["continent_name"] = geoip.city.Continent.Names[lang]
			}
		case "latitude":
			if geoip.city != nil {
				data["latitude"] = geoip.city.Location.Latitude
			}
		case "longitude":
			if geoip.city != nil {
				data["longitude"] = geoip.city.Location.Longitude
			}
		case "metro_code":
			if geoip.city != nil {
				data["metro_code"] = geoip.city.Location.MetroCode
			}
		case "timezone":
			if geoip.city != nil {
				data["timezone"] = geoip.city.Location.TimeZone
			}
		case "postal_code":
			if geoip.city != nil {
				data["postal_code"] = geoip.city.Postal.Code
			}
		case "region_code":
			if geoip.city != nil {
				data["region_code"] = geoip.city.Subdivisions[0].IsoCode
			}
		case "region_name":
			if geoip.city != nil {
				data["region_name"] = geoip.city.Subdivisions[0].Names[lang]
			}
		case "is_anonymous_proxy":
			if geoip.city != nil {
				data["is_anonymous_proxy"] = geoip.city.Traits.IsAnonymousProxy
			}
		case "is_satellite_provider":
			if geoip.city != nil {
				data["is_satellite_provider"] = geoip.city.Traits.IsSatelliteProvider
			}

		// ISP database
		case "asn":
			if geoip.isp != nil {
				data["asn"] = geoip.isp.AutonomousSystemNumber
			}
		case "organization":
			if geoip.isp != nil {
				data["organization"] = geoip.isp.AutonomousSystemOrganization
			}
		case "isp":
			if geoip.isp != nil {
				data["isp"] = geoip.isp.ISP
			}
		}
	}

	if len(p.opt.Target) > 0 {
		e.Fields().SetValueForPath(data, p.opt.Target)
	} else {
		for k, v := range data {
			e.Fields().SetValueForPath(v, k)
		}
	}

	processors.ProcessCommonFields2(e.Fields(),
		p.opt.AddField,
		p.opt.AddTag,
		p.opt.RemoveField,
		p.opt.RemoveTag,
	)

	p.Send(e, 0)
	return nil
}

func (p *processor) load(databases interface{}) (err error) {
	for name, path := range databases.(map[string]interface{}) {
		p.databases[name], err = geoip2.Open(path.(string))
		if err != nil {
			return err
		}
	}
	if len(p.databases) == 0 {
		return errors.New("no valid GeoIP database found")
	}
	return nil
}

func (p *processor) getInfo() func(ip string) (lrucache.Cacheable, error) {
	return func(ip string) (lrucache.Cacheable, error) {
		records := geoipRecords{}
		netIP := net.ParseIP(ip)
		if netIP == nil {
			return nil, errors.New("no valid IP address found")
		}

		for name, db := range p.databases {
			switch strings.ToLower(name) {
			case "city":
				fmt.Println("-=-=-=-")
				if record, err := db.City(netIP); err == nil {
					records.city = record
				}
			case "isp":
				if record, err := db.ISP(netIP); err == nil {
					records.isp = record
				}
				//case "country":
				//case "domain":
				//case "anonymousip":
			}
		}
		return records, nil
	}
}

func (p *processor) Tick(e veino.IPacket) error  { return nil }
func (p *processor) Start(e veino.IPacket) error { return nil }
func (p *processor) Stop(e veino.IPacket) error  { return nil }
