package null

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/veino/veino/testutils"
)

func TestNew(t *testing.T) {
	p := New(nil)
	_, ok := p.(*processor)
	assert.Equal(t, ok, true, "New(nil) should return a processor struct")
}

func TestConfigure(t *testing.T) {
	p := New(nil).(*processor)
	conf := map[string]interface{}{}
	ret := p.Configure(conf)
	assert.Equal(t, ret, nil, "")
}

func TestReceive(t *testing.T) {
	p := New(nil)
	em := testutils.NewTestEvent("sourceAgentName", "a log message", nil)

	ret := p.Receive(&em)
	assert.Equal(t, nil, ret, "")
	em.AssertNotCalled(t, "Pipe")
	em.AssertNotCalled(t, "Send")
}

func TestStart(t *testing.T) {
	p := New(nil)
	em := testutils.NewTestEvent("sourceAgentName", "a log message", nil)

	ret := p.Start(&em)
	assert.Equal(t, nil, ret, "")
	em.AssertNotCalled(t, "Pipe")
	em.AssertNotCalled(t, "Send")
}

func TestStop(t *testing.T) {
	p := New(nil)
	em := testutils.NewTestEvent("sourceAgentName", "a log message", nil)

	ret := p.Stop(&em)
	assert.Equal(t, nil, ret, "")
	em.AssertNotCalled(t, "Pipe")
	em.AssertNotCalled(t, "Send")
}

func TestTick(t *testing.T) {
	p := New(nil)
	em := testutils.NewTestEvent("sourceAgentName", "a log message", nil)

	ret := p.Tick(&em)
	assert.Equal(t, nil, ret, "")
	em.AssertNotCalled(t, "Pipe")
	em.AssertNotCalled(t, "Send")
}
