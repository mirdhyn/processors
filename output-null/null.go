// Drops everything received
package null

import (
	"github.com/veino/processors"
	"github.com/veino/veino"
)

func New() veino.Processor {
	return &processor{}
}

type processor struct {
	processors.Base
}
