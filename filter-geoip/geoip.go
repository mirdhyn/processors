package geoip

import (
	"errors"
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

	// Path or url to the GeoIP database (can be gziped).
	// Default value is "http://geolite.maxmind.com/download/geoip/database/GeoLite2-City.mmdb.gz".
	Database string `mapstructure:"database"`

	// GeoIP database type. Default value is "city".
	// Accepted value can be one of "city", "isp", "country" "domain" or "anonymousip"
	Type string `mapstructure:"type"`

	// GeoIP database update interval (in minutes). Default value is 0 (no updates).
	// If `database` field contains an url, the database will be retrieved from this url at specified interval.
	// ETag header is checked and new database will be downloaded only if necessary.
	// If `database` field is a local path, the database will be re-loaded at specified interval.
	// Note: the update process clear the cache and can impact performance.
	UpdateInterval int `mapstructure:"update_interval"`

	// An array of geoip fields to be included in the event.
	// Possible fields depend on the database type. By default, all geoip fields are included in the event.
	Fields []string `mapstructure:"fields"`

	// Cache size
	// default 1000
	LruCacheSize int64 `mapstructure:"lru_cache_size"`
	CacheSize    int64 `mapstructure:"cache_size"`

	// If this filter is successful, remove arbitrary fields from this event.
	RemoveField []string `mapstructure:"remove_field"`

	// If this filter is successful, remove arbitrary tags from the event.
	// Tags can be dynamic and include parts of the event using the %{field} syntax
	RemoveTag []string `mapstructure:"remove_tag"`

	// The field containing the IP address or hostname to map via geoip.
	Source string `mapstructure:"source" validate:"required"`

	// Define the target field for placing the parsed data. If this setting is omitted,
	// the geoip data will be stored at the root (top level) of the event
	Target string `mapstructure:"target"`

	// Language to use for city/region/continent names
	Language string `mapstructure:"language"`
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
		Language:       "en",
		CacheSize:      1000,
		Target:         "geoip",
		Type:           "city",
		UpdateInterval: 0,
		Database:       "http://geolite.maxmind.com/download/geoip/database/GeoLite2-City.mmdb.gz",
	}
	p.opt = &defaults

	err := p.ConfigureAndValidate(ctx, conf, p.opt)

	if p.opt.LruCacheSize > 0 {
		p.opt.CacheSize = p.opt.LruCacheSize
	}

	p.cache = lrucache.New(p.opt.CacheSize)
	p.databases = map[string]*geoip2.Reader{}

	if err == nil {

		if p.opt.Database != "" {
			p.opt.Databases["city"] = p.opt.Database
		}

		err = p.load(p.opt.Databases)
		if err != nil {
			p.Logger.Println(err)
		}
	}

	return err
}

func (p *processor) Receive(e veino.IPacket) error {
	ip, err := e.Fields().ValueForPathString(p.opt.Source)

	if err != nil {
		return err
	}

	p.cache.OnMiss(p.getInfo())

	records, err := p.cache.Get(ip)
	if err != nil {
		return err
	}

	geoip := records.(geoipRecords)

	data := make(map[string]interface{})
	lang := p.opt.Language

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

	if p.opt.Target != "" {
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

func (p *processor) load(databases map[string]string) (err error) {
	if databases == nil {
		return errors.New("no valid GeoIP database found")
	}
	for name, path := range databases {
		p.databases[name], err = geoip2.Open(path)
		if err != nil {
			return err
		}
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
			case "isp":
				if record, err := db.ISP(netIP); err == nil {
					records.isp = record
				}

			//case "country":
			//case "domain":
			//case "anonymousip":

			default:
				if record, err := db.City(netIP); err == nil {
					records.city = record
				}
			}
		}
		return records, nil
	}
}

func (p *processor) Tick(e veino.IPacket) error  { return nil }
func (p *processor) Start(e veino.IPacket) error { return nil }
func (p *processor) Stop(e veino.IPacket) error  { return nil }
